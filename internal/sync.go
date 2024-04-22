// Copyright (c) 2020, Amazon.com, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package internal ...
package internal

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/awslabs/ssosync/internal/aws"
	"github.com/awslabs/ssosync/internal/config"
	"github.com/awslabs/ssosync/internal/google"
	"github.com/hashicorp/go-retryablehttp"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	admin "google.golang.org/api/admin/directory/v1"
)

// SyncGSuite is the interface for synchronizing users/groups
type SyncGSuite interface {
	SyncUsers(string) error
	SyncGroups(string) error
	SyncGroupsUsers(string) error
}

// SyncGSuite is an object type that will synchronize real users and groups
type syncGSuite struct {
	aws    aws.Client
	google google.Client
	cfg    *config.Config

	users map[string]*aws.User
}

// New will create a new SyncGSuite object
func New(cfg *config.Config, a aws.Client, g google.Client) SyncGSuite {
	return &syncGSuite{
		aws:    a,
		google: g,
		cfg:    cfg,
		users:  make(map[string]*aws.User),
	}
}

// SyncUsers will Sync Google Users to AWS SSO SCIM
// References:
// * https://developers.google.com/admin-sdk/directory/v1/guides/search-users
// query possible values:
// '' --> empty or not defined
//  name:'Jane'
//  email:admin*
//  isAdmin=true
//  manager='janesmith@example.com'
//  orgName=Engineering orgTitle:Manager
//  EmploymentData.projects:'GeneGnomes'
func (s *syncGSuite) SyncUsers(query string) error {
	log.Debug("get deleted users")
	deletedUsers, err := s.google.GetDeletedUsers()
   if err != nil {
	log.Warn("Error Getting Deleted Users")
   return err
	}
   for _, u := range deletedUsers {
	log.WithFields(log.Fields{
   "email": u.PrimaryEmail,
	}).Info("deleting google user")
	uu, err := s.aws.FindUserByEmail(u.PrimaryEmail)
   if err != aws.ErrUserNotFound && err != nil {
	log.WithFields(log.Fields{
   "email": u.PrimaryEmail,
	}).Warn("Error deleting google user")
   return err
	}
   if err == aws.ErrUserNotFound {
	log.WithFields(log.Fields{
   "email": u.PrimaryEmail,
	}).Debug("User already deleted")
   continue
	}
	log.WithFields(log.Fields{
   "email":    u.PrimaryEmail,
   "username": uu.Username,
   "id":       uu.ID,
	}).Info("Deleting user in AWS")
   if err := s.aws.DeleteUser(uu); err != nil {
	log.WithFields(log.Fields{
   "email":    u.PrimaryEmail,
   "username": uu.Username,
   "id":       uu.ID,
	}).Warn("Error deleting user")
   return err
	}
	log.WithFields(log.Fields{
   "email":    u.PrimaryEmail,
   "username": uu.Username,
   "id":       uu.ID,
	}).Info("User deleted successfully in AWS")
	}
	log.Debug("get active google users")
	googleUsers, err := s.google.GetUsers(query)
   if err != nil {
	log.WithField("query", query).Warn("Error getting active Google users")
   return err
	}
	log.WithField("count", len(googleUsers)).Info("Active Google users retrieved")
   for _, u := range googleUsers {
   if s.ignoreUser(u.PrimaryEmail) {
	log.WithField("email", u.PrimaryEmail).Debug("Ignoring user based on configuration")
   continue
	}
	ll := log.WithFields(log.Fields{
   "email": u.PrimaryEmail,
	})
	ll.Debug("finding user")
	uu, _ := s.aws.FindUserByEmail(u.PrimaryEmail)
   if uu != nil {
	s.users[uu.Username] = uu
   // Update the user when suspended state is changed
   if uu.Active == u.Suspended {
	log.WithFields(log.Fields{
   "email":    u.PrimaryEmail,
   "username": uu.Username,
   "id":       uu.ID,
	}).Info("Mismatch active/suspended, updating user")
   // create new user object and update the user
	_, err := s.aws.UpdateUser(aws.UpdateUser(
	uu.ID,
	u.Name.GivenName,
	u.Name.FamilyName,
	u.PrimaryEmail,
   !u.Suspended))
   if err != nil {
	log.WithFields(log.Fields{
   "email":    u.PrimaryEmail,
   "username": uu.Username,
   "id":       uu.ID,
	}).Warn("Error updating user")
   return err
	}
	log.WithFields(log.Fields{
   "email":    u.PrimaryEmail,
   "username": uu.Username,
   "id":       uu.ID,
	}).Info("User updated successfully")
	}
   continue
	}
	ll.Info("creating user")
	log.WithFields(log.Fields{
   "email":      u.PrimaryEmail,
   "givenName":  u.Name.GivenName,
   "familyName": u.Name.FamilyName,
   "suspended":  u.Suspended,
	}).Info("Creating user in AWS")
	uu, err := s.aws.CreateUser(aws.NewUser(
	u.Name.GivenName,
	u.Name.FamilyName,
	u.PrimaryEmail,
   !u.Suspended))
   if err != nil {
	log.WithFields(log.Fields{
   "email":      u.PrimaryEmail,
   "givenName":  u.Name.GivenName,
   "familyName": u.Name.FamilyName,
   "suspended":  u.Suspended,
	}).Warn("Error creating user")
   return err
	}
	log.WithFields(log.Fields{
   "email":    uu.Username,
   "username": uu.Username,
   "id":       uu.ID,
	}).Info("User created successfully in AWS")
	s.users[uu.Username] = uu
	}
   return nil
   }

// SyncGroups will sync groups from Google -> AWS SSO
// References:
// * https://developers.google.com/admin-sdk/directory/v1/guides/search-groups
// query possible values:
// '' --> empty or not defined
//  name='contact'
//  email:admin*
//  memberKey=user@company.com
//  name:contact* email:contact*
//  name:Admin* email:aws-*
//  email:aws-*
func (s *syncGSuite) SyncGroups(query string) error {
	log.WithField("query", query).Debug("get google groups")
	googleGroups, err := s.google.GetGroups(query)
   if err != nil {
	log.WithField("query", query).Warn("Error getting Google groups")
   return err
	}
	log.WithField("count", len(googleGroups)).Info("Google groups retrieved")
	correlatedGroups := make(map[string]*aws.Group)
   for _, g := range googleGroups {
   if s.ignoreGroup(g.Email) || !s.includeGroup(g.Email) {
	log.WithField("group", g.Email).Debug("Ignoring group based on configuration")
   continue
	}
	log := log.WithFields(log.Fields{
   "group": g.Email,
	})
	log.Debug("Check group")
   var group *aws.Group
	gg, err := s.aws.FindGroupByDisplayName(g.Email)
   if err != nil && err != aws.ErrGroupNotFound {
	log.WithField("group", g.Email).Warn("Error finding group in AWS")
   return err
	}
   if gg != nil {
	log.Debug("Found group")
	correlatedGroups[gg.DisplayName] = gg
	group = gg
	} else {
	log.Info("Creating group in AWS")
	newGroup, err := s.aws.CreateGroup(aws.NewGroup(g.Email))
   if err != nil {
	log.WithField("group", g.Email).Warn("Error creating group in AWS")
   return err
	}
	log.WithFields(logrus.Fields{
   "group":       newGroup.DisplayName,
   "id":          newGroup.ID,
	}).Info("Group created successfully in AWS")
	correlatedGroups[newGroup.DisplayName] = newGroup
	group = newGroup
	}
	groupMembers, err := s.google.GetGroupMembers(g)
   if err != nil {
	log.WithField("group", g.Email).Warn("Error getting group members from Google")
   return err
	}
	log.WithFields(logrus.Fields{
   "group": g.Email,
   "count": len(groupMembers),
	}).Info("Group members retrieved from Google")
	memberList := make(map[string]*admin.Member)
	log.Info("Start group user sync")
   for _, m := range groupMembers {
   if _, ok := s.users[m.Email]; ok {
	memberList[m.Email] = m
	}
	}
   for _, u := range s.users {
	log.WithField("user", u.Username).Debug("Checking user is in group already")
	b, err := s.aws.IsUserInGroup(u, group)
   if err != nil {
	log.WithFields(logrus.Fields{
   "user":  u.Username,
   "group": group.DisplayName,
	}).Warn("Error checking user membership in AWS group")
   return err
	}
   if _, ok := memberList[u.Username]; ok {
   if !b {
	log.WithFields(logrus.Fields{
   "user":  u.Username,
   "group": group.DisplayName,
	}).Info("Adding user to group")
	err := s.aws.AddUserToGroup(u, group)
   if err != nil {
	log.WithFields(logrus.Fields{
   "user":  u.Username,
   "group": group.DisplayName,
	}).Warn("Error adding user to group in AWS")
   return err
	}
	log.WithFields(logrus.Fields{
   "user":  u.Username,
   "group": group.DisplayName,
	}).Info("User added to group successfully in AWS")
	}
	} else {
   if b {
	log.WithFields(logrus.Fields{
   "user":  u.Username,
   "group": group.DisplayName,
	}).Warn("Removing user from group")
	err := s.aws.RemoveUserFromGroup(u, group)
   if err != nil {
	log.WithFields(logrus.Fields{
   "user":  u.Username,
   "group": group.DisplayName,
	}).Warn("Error removing user from group in AWS")
   return err
	}
	log.WithFields(logrus.Fields{
   "user":  u.Username,
   "group": group.DisplayName,
	}).Info("User removed from group successfully in AWS")
	}
	}
	}
	}
   return nil
   }

// SyncGroupsUsers will sync groups and its members from Google -> AWS SSO SCIM
// allowing filter groups base on google api filter query parameter
// References:
// * https://developers.google.com/admin-sdk/directory/v1/guides/search-groups
// query possible values:
// '' --> empty or not defined
//  name='contact'
//  email:admin*
//  memberKey=user@company.com
//  name:contact* email:contact*
//  name:Admin* email:aws-*
//  email:aws-*
// process workflow:
//  1) delete users in aws, these were deleted in google
//  2) update users in aws, these were updated in google
//  3) add users in aws, these were added in google
//  4) add groups in aws and add its members, these were added in google
//  5) validate equals aws an google groups members
//  6) delete groups in aws, these were deleted in google
func (s *syncGSuite) SyncGroupsUsers(query string) error {
	log.WithField("query", query).Info("get google groups")
	googleGroups, err := s.google.GetGroups(query)
   if err != nil {
	log.WithField("query", query).Warn("Error getting Google groups")
   return err
	}
	log.WithField("count", len(googleGroups)).Info("Google groups retrieved")
	filteredGoogleGroups := []*admin.Group{}
   for _, g := range googleGroups {
   if s.ignoreGroup(g.Email) {
	log.WithField("group", g.Email).Debug("ignoring group")
   continue
	}
	filteredGoogleGroups = append(filteredGoogleGroups, g)
	}
	googleGroups = filteredGoogleGroups
	log.Debug("preparing list of google users and then google groups and their members")
	googleUsers, googleGroupsUsers, err := s.getGoogleGroupsAndUsers(googleGroups)
   if err != nil {
	log.Warn("Error getting Google groups and users")
   return err
	}
	log.WithFields(log.Fields{
   "googleUsers":  len(googleUsers),
   "googleGroups": len(googleGroupsUsers),
	}).Info("Google users and groups retrieved")
	log.Info("get existing aws groups")
	awsGroups, err := s.aws.GetGroups()
   if err != nil {
	log.Error("error getting aws groups")
   return err
	}
	log.WithField("count", len(awsGroups)).Info("AWS groups retrieved")
	log.Info("get existing aws users")
	awsUsers, err := s.aws.GetUsers()
   if err != nil {
	log.Error("error getting aws users")
   return err
	}
	log.WithField("count", len(awsUsers)).Info("AWS users retrieved")
	log.Debug("preparing list of aws groups and their members")
	awsGroupsUsers, err := s.getAWSGroupsAndUsers(awsGroups, awsUsers)
   if err != nil {
	log.Warn("Error getting AWS groups and users")
   return err
	}
	log.WithField("count", len(awsGroupsUsers)).Info("AWS groups and users retrieved")
   // create list of changes by operations
	addAWSUsers, delAWSUsers, updateAWSUsers, _ := getUserOperations(awsUsers, googleUsers)
	addAWSGroups, delAWSGroups, equalAWSGroups := getGroupOperations(awsGroups, googleGroups)
	log.WithFields(log.Fields{
   "addAWSUsers":    len(addAWSUsers),
   "delAWSUsers":    len(delAWSUsers),
   "updateAWSUsers": len(updateAWSUsers),
   "addAWSGroups":   len(addAWSGroups),
   "delAWSGroups":   len(delAWSGroups),
   "equalAWSGroups": len(equalAWSGroups),
	}).Info("Changes to be applied")
	log.Info("syncing changes")
   // delete aws users (deleted in google)
	log.Debug("deleting aws users deleted in google")
   for _, awsUser := range delAWSUsers {
	log := log.WithFields(log.Fields{"user": awsUser.Username})
	log.Debug("finding user")
	awsUserFull, err := s.aws.FindUserByEmail(awsUser.Username)
   if err != nil {
	log.Warn("Error finding user in AWS")
   return err
	}
	log.Warn("deleting user")
   if err := s.aws.DeleteUser(awsUserFull); err != nil {
	log.Error("error deleting user")
   return err
	}
	log.Info("User deleted successfully in AWS")
	}
   // update aws users (updated in google)
	log.Debug("updating aws users updated in google")
   for _, awsUser := range updateAWSUsers {
	log := log.WithFields(log.Fields{"user": awsUser.Username})
	log.Debug("finding user")
	awsUserFull, err := s.aws.FindUserByEmail(awsUser.Username)
   if err != nil {
	log.Warn("Error finding user in AWS")
   return err
	}
	log.Warn("updating user")
	_, err = s.aws.UpdateUser(awsUserFull)
   if err != nil {
	log.Error("error updating user")
   return err
	}
	log.Info("User updated successfully in AWS")
	}
   // add aws users (added in google)
	log.Debug("creating aws users added in google")
   for _, awsUser := range addAWSUsers {
	log := log.WithFields(log.Fields{"user": awsUser.Username})
	log.Info("creating user")
	_, err := s.aws.CreateUser(awsUser)
   if err != nil {
	errHttp := new(aws.ErrHttpNotOK)
   if errors.As(err, &errHttp) && errHttp.StatusCode == 409 {
	log.WithField("user", awsUser.Username).Warn("user already exists")
   continue
	}
	log.Error("error creating user")
   return err
	}
	log.Info("User created successfully in AWS")
	}
   // add aws groups (added in google)
	log.Debug("creating aws groups added in google")
   for _, awsGroup := range addAWSGroups {
	log := log.WithFields(log.Fields{"group": awsGroup.DisplayName})
	log.Info("creating group")
	_, err := s.aws.CreateGroup(awsGroup)
   if err != nil {
	log.Error("creating group")
   return err
	}
	log.Info("Group created successfully in AWS")
   // add members of the new group
   for _, googleUser := range googleGroupsUsers[awsGroup.DisplayName] {
   // equivalent aws user of google user on the fly
	log.Debug("finding user")
	awsUserFull, err := s.aws.FindUserByEmail(googleUser.PrimaryEmail)
   if err != nil {
	log.WithField("email", googleUser.PrimaryEmail).Warn("Error finding user in AWS")
   return err
	}
	log.WithField("user", awsUserFull.Username).Info("adding user to group")
	err = s.aws.AddUserToGroup(awsUserFull, awsGroup)
   if err != nil {
	log.WithFields(logrus.Fields{
   "user":  awsUserFull.Username,
   "group": awsGroup.DisplayName,
	}).Warn("Error adding user to group in AWS")
   return err
	}
	log.WithFields(logrus.Fields{
   "user":  awsUserFull.Username,
   "group": awsGroup.DisplayName,
	}).Info("User added to group successfully in AWS")
	}
	}
   // list of users to to be removed in aws groups
	deleteUsersFromGroup, _ := getGroupUsersOperations(googleGroupsUsers, awsGroupsUsers)
   // validate groups members are equal in aws and google
	log.Debug("validating groups members, equals in aws and google")
   for _, awsGroup := range equalAWSGroups {
   // add members of the new group
	log := log.WithFields(log.Fields{"group": awsGroup.DisplayName})
   for _, googleUser := range googleGroupsUsers[awsGroup.DisplayName] {
	log.WithField("user", googleUser.PrimaryEmail).Debug("finding user")
	awsUserFull, err := s.aws.FindUserByEmail(googleUser.PrimaryEmail)
   if err != nil {
	log.WithField("email", googleUser.PrimaryEmail).Warn("Error finding user in AWS")
   return err
	}
	log.WithField("user", awsUserFull.Username).Debug("checking user is in group already")
	b, err := s.aws.IsUserInGroup(awsUserFull, awsGroup)
   if err != nil {
	log.WithFields(logrus.Fields{
   "user":  awsUserFull.Username,
   "group": awsGroup.DisplayName,
	}).Warn("Error checking user membership in AWS group")
   return err
	}
   if !b {
	log.WithField("user", awsUserFull.Username).Info("adding user to group")
	err := s.aws.AddUserToGroup(awsUserFull, awsGroup)
   if err != nil {
	log.WithFields(logrus.Fields{
   "user":  awsUserFull.Username,
   "group": awsGroup.DisplayName,
	}).Warn("Error adding user to group in AWS")
   return err
	}
	log.WithFields(logrus.Fields{
   "user":  awsUserFull.Username,
   "group": awsGroup.DisplayName,
	}).Info("User added to group successfully in AWS")
	}
	}
   for _, awsUser := range deleteUsersFromGroup[awsGroup.DisplayName] {
	log.WithField("user", awsUser.Username).Warn("removing user from group")
	err := s.aws.RemoveUserFromGroup(awsUser, awsGroup)
   if err != nil {
	log.WithFields(logrus.Fields{
   "user":  awsUser.Username,
   "group": awsGroup.DisplayName,
	}).Warn("Error removing user from group in AWS")
   return err
	}
	log.WithFields(logrus.Fields{
   "user":  awsUser.Username,
   "group": awsGroup.DisplayName,
	}).Info("User removed from group successfully in AWS")
	}
	}
   // delete aws groups (deleted in google)
	log.Debug("delete aws groups deleted in google")
   for _, awsGroup := range delAWSGroups {
	log := log.WithFields(log.Fields{"group": awsGroup.DisplayName})
	log.Debug("finding group")
	awsGroupFull, err := s.aws.FindGroupByDisplayName(awsGroup.DisplayName)
   if err != nil {
	log.WithField("group", awsGroup.DisplayName).Warn("Error finding group in AWS")
   return err
	}
	log.Warn("deleting group")
	err = s.aws.DeleteGroup(awsGroupFull)
   if err != nil {
	log.Error("deleting group")
   return err
	}
	log.Info("Group deleted successfully in AWS")
	}
	log.Info("sync completed")
   return nil
   }

// getGoogleGroupsAndUsers return a list of google users members of googleGroups
// and a map of google groups and its users' list
func (s *syncGSuite) getGoogleGroupsAndUsers(googleGroups []*admin.Group) ([]*admin.User, map[string][]*admin.User, error) {
	log.WithField("count", len(googleGroups)).Info("Getting Google groups and users")
	gUsers := make([]*admin.User, 0)
	gGroupsUsers := make(map[string][]*admin.User)
	gUniqUsers := make(map[string]*admin.User)
   for _, g := range googleGroups {
	log := log.WithFields(log.Fields{"group": g.Name})
   if s.ignoreGroup(g.Email) {
	log.Debug("ignoring group")
   continue
	}
	log.Debug("get group members from google")
	groupMembers, err := s.google.GetGroupMembers(g)
   if err != nil {
	log.WithField("group", g.Email).Warn("Error getting group members from Google")
   return nil, nil, err
	}
	log.WithField("count", len(groupMembers)).Info("Group members retrieved from Google")
	log.Debug("get users")
	membersUsers := make([]*admin.User, 0)
   for _, m := range groupMembers {
   if s.ignoreUser(m.Email) {
	log.WithField("id", m.Email).Debug("ignoring user")
   continue
	}
   if m.Type == "GROUP" {
	log.WithField("id", m.Email).Debug("ignoring group address")
   continue
	}
	log.WithField("id", m.Email).Debug("get user")
	q := fmt.Sprintf("email:%s", m.Email)
	u, err := s.google.GetUsers(q) // TODO: implement GetUser(m.Email)
   if err != nil {
	log.WithField("email", m.Email).Warn("Error getting user from Google")
   return nil, nil, err
	}
   if len(u) == 0 {
	log.WithField("email", m.Email).Debug("Ignoring Unknown User")
   continue
	}
	log.WithFields(logrus.Fields{
   "email":      u[0].PrimaryEmail,
   "givenName":  u[0].Name.GivenName,
   "familyName": u[0].Name.FamilyName,
	}).Info("User retrieved from Google")
	membersUsers = append(membersUsers, u[0])
	_, ok := gUniqUsers[m.Email]
   if !ok {
	gUniqUsers[m.Email] = u[0]
	}
	}
	gGroupsUsers[g.Name] = membersUsers
	log.WithFields(logrus.Fields{
   "group": g.Name,
   "count": len(membersUsers),
	}).Info("Group members added to map")
	}
   for _, user := range gUniqUsers {
	gUsers = append(gUsers, user)
	}
	log.WithFields(log.Fields{
   "uniqueUsers": len(gUniqUsers),
   "totalUsers":  len(gUsers),
	}).Info("Google users retrieved")
   return gUsers, gGroupsUsers, nil
   }

// getAWSGroupsAndUsers return a list of google users members of googleGroups
// and a map of google groups and its users' list
func (s *syncGSuite) getAWSGroupsAndUsers(awsGroups []*aws.Group, awsUsers []*aws.User) (map[string][]*aws.User, error) {
	log.WithFields(log.Fields{
   "groups": len(awsGroups),
   "users":  len(awsUsers),
	}).Info("Getting AWS groups and users")
	awsGroupsUsers := make(map[string][]*aws.User)
   for _, awsGroup := range awsGroups {
	users := make([]*aws.User, 0)
	log := log.WithFields(log.Fields{"group": awsGroup.DisplayName})
	log.Debug("get group members from aws")
   // NOTE: AWS has not implemented yet some method to get the groups members https://docs.aws.amazon.com/singlesignon/latest/developerguide/listgroups.html
   // so, we need to check each user in each group which are too many unnecessary API calls
   for _, user := range awsUsers {
	log.Debug("checking if user is member of")
	found, err := s.aws.IsUserInGroup(user, awsGroup)
   if err != nil {
	log.WithFields(logrus.Fields{
   "user":  user.Username,
   "group": awsGroup.DisplayName,
	}).Warn("Error checking user membership in AWS group")
   return nil, err
	}
   if found {
	users = append(users, user)
	log.WithField("user", user.Username).Debug("User is a member of the group")
	}
	}
	awsGroupsUsers[awsGroup.DisplayName] = users
	log.WithField("count", len(users)).Info("Group members added to map")
	}
	log.WithField("count", len(awsGroupsUsers)).Info("AWS groups and users retrieved")
   return awsGroupsUsers, nil
   }

// getGroupOperations returns the groups of AWS that must be added, deleted and are equals
func getGroupOperations(awsGroups []*aws.Group, googleGroups []*admin.Group) (add []*aws.Group, delete []*aws.Group, equals []*aws.Group) {
	log.WithFields(log.Fields{
   "awsGroups":    len(awsGroups),
   "googleGroups": len(googleGroups),
	}).Info("Getting group operations")
	awsMap := make(map[string]*aws.Group)
	googleMap := make(map[string]struct{})
   for _, awsGroup := range awsGroups {
	awsMap[awsGroup.DisplayName] = awsGroup
	}
   for _, gGroup := range googleGroups {
	googleMap[gGroup.Name] = struct{}{}
	}
   // AWS Groups found and not found in google
   for _, gGroup := range googleGroups {
   if _, found := awsMap[gGroup.Name]; found {
	log.WithField("group", gGroup.Name).Debug("Group found in AWS and Google")
	equals = append(equals, awsMap[gGroup.Name])
	} else {
	log.WithField("group", gGroup.Name).Info("Group not found in AWS, will be added")
	add = append(add, aws.NewGroup(gGroup.Name))
	}
	}
   // Google Groups founds and not in aws
   for _, awsGroup := range awsGroups {
   if _, found := googleMap[awsGroup.DisplayName]; !found {
	log.WithField("group", awsGroup.DisplayName).Info("Group not found in Google, will be deleted from AWS")
	delete = append(delete, aws.NewGroup(awsGroup.DisplayName))
	}
	}
	log.WithFields(log.Fields{
   "add":    len(add),
   "delete": len(delete),
   "equals": len(equals),
	}).Info("Group operations determined")
   return add, delete, equals
   }

// getUserOperations returns the users of AWS that must be added, deleted, updated and are equals
func getUserOperations(awsUsers []*aws.User, googleUsers []*admin.User) (add []*aws.User, delete []*aws.User, update []*aws.User, equals []*aws.User) {
	log.WithFields(log.Fields{
   "awsUsers":    len(awsUsers),
   "googleUsers": len(googleUsers),
	}).Info("Getting user operations")
	awsMap := make(map[string]*aws.User)
	googleMap := make(map[string]struct{})
   for _, awsUser := range awsUsers {
	awsMap[awsUser.Username] = awsUser
	}
   for _, gUser := range googleUsers {
	googleMap[gUser.PrimaryEmail] = struct{}{}
	}
   // AWS Users found and not found in google
   for _, gUser := range googleUsers {
   if awsUser, found := awsMap[gUser.PrimaryEmail]; found {
	log.WithField("user", gUser.PrimaryEmail).Debug("User found in AWS and Google")
   if awsUser.Active == gUser.Suspended ||
	awsUser.Name.GivenName != gUser.Name.GivenName ||
	awsUser.Name.FamilyName != gUser.Name.FamilyName {
	log.WithFields(log.Fields{
   "user":       gUser.PrimaryEmail,
   "givenName":  gUser.Name.GivenName,
   "familyName": gUser.Name.FamilyName,
   "suspended":  gUser.Suspended,
	}).Info("User attributes mismatch, will be updated in AWS")
	update = append(update, aws.NewUser(gUser.Name.GivenName, gUser.Name.FamilyName, gUser.PrimaryEmail, !gUser.Suspended))
	} else {
	log.WithField("user", gUser.PrimaryEmail).Debug("User attributes match in AWS and Google")
	equals = append(equals, awsUser)
	}
	} else {
	log.WithFields(log.Fields{
   "user":       gUser.PrimaryEmail,
   "givenName":  gUser.Name.GivenName,
   "familyName": gUser.Name.FamilyName,
   "suspended":  gUser.Suspended,
	}).Info("User not found in AWS, will be added")
	add = append(add, aws.NewUser(gUser.Name.GivenName, gUser.Name.FamilyName, gUser.PrimaryEmail, !gUser.Suspended))
	}
	}
   // Google Users founds and not in aws
   for _, awsUser := range awsUsers {
   if _, found := googleMap[awsUser.Username]; !found {
	log.WithFields(log.Fields{
   "user":       awsUser.Username,
   "givenName":  awsUser.Name.GivenName,
   "familyName": awsUser.Name.FamilyName,
   "active":     awsUser.Active,
	}).Info("User not found in Google, will be deleted from AWS")
	delete = append(delete, aws.NewUser(awsUser.Name.GivenName, awsUser.Name.FamilyName, awsUser.Username, awsUser.Active))
	}
	}
	log.WithFields(log.Fields{
   "add":    len(add),
   "delete": len(delete),
   "update": len(update),
   "equals": len(equals),
	}).Info("User operations determined")
   return add, delete, update, equals
   }

// groupUsersOperations returns the groups and its users of AWS that must be delete from these groups and what are equals
func getGroupUsersOperations(gGroupsUsers map[string][]*admin.User, awsGroupsUsers map[string][]*aws.User) (delete map[string][]*aws.User, equals map[string][]*aws.User) {
	log.WithFields(log.Fields{
   "googleGroups": len(gGroupsUsers),
   "awsGroups":    len(awsGroupsUsers),
	}).Info("Getting group users operations")
	mbG := make(map[string]map[string]struct{})
   // get user in google groups that are in aws groups and
   // users in aws groups that aren't in google groups
   for gGroupName, gGroupUsers := range gGroupsUsers {
	mbG[gGroupName] = make(map[string]struct{})
   for _, gUser := range gGroupUsers {
	mbG[gGroupName][gUser.PrimaryEmail] = struct{}{}
	}
	}
	delete = make(map[string][]*aws.User)
	equals = make(map[string][]*aws.User)
   for awsGroupName, awsGroupUsers := range awsGroupsUsers {
   for _, awsUser := range awsGroupUsers {
   // users that exist in aws groups but doesn't in google groups
   if _, found := mbG[awsGroupName][awsUser.Username]; found {
	log.WithFields(log.Fields{
   "user":  awsUser.Username,
   "group": awsGroupName,
	}).Debug("User found in AWS group and Google group")
	equals[awsGroupName] = append(equals[awsGroupName], awsUser)
	} else {
	log.WithFields(log.Fields{
   "user":  awsUser.Username,
   "group": awsGroupName,
	}).Info("User found in AWS group but not in Google group, will be removed from AWS group")
	delete[awsGroupName] = append(delete[awsGroupName], awsUser)
	}
	}
	}
	log.WithFields(log.Fields{
   "delete": len(delete),
   "equals": len(equals),
	}).Info("Group users operations determined")
   return
   }

// DoSync will create a logger and run the sync with the paths
// given to do the sync.
func DoSync(ctx context.Context, cfg *config.Config) error {
	log.Info("Starting synchronization process")
	log.Info("Syncing AWS users and groups from Google Workspace SAML Application")
	creds := []byte(cfg.GoogleCredentials)
   if !cfg.IsLambda {
	b, err := ioutil.ReadFile(cfg.GoogleCredentials)
   if err != nil {
	log.WithError(err).Error("Error reading Google credentials file")
   return err
	}
	creds = b
	}
   // create a http client with retry and backoff capabilities
	retryClient := retryablehttp.NewClient()
   // https://github.com/hashicorp/go-retryablehttp/issues/6
   if cfg.Debug {
	retryClient.Logger = log.StandardLogger()
	} else {
	retryClient.Logger = nil
	}
	httpClient := retryClient.StandardClient()
	googleClient, err := google.NewClient(ctx, cfg.GoogleAdmin, creds, cfg.GoogleCustomerId)
   if err != nil {
	log.WithError(err).Error("Error creating Google client")
   return err
	}
	log.Info("Google client created successfully")
	awsClient, err := aws.NewClient(
	httpClient,
   &aws.Config{
	Endpoint: cfg.SCIMEndpoint,
	Token: cfg.SCIMAccessToken,
	})
   if err != nil {
	log.WithError(err).Error("Error creating AWS client")
   return err
	}
	log.Info("AWS client created successfully")
	c := New(cfg, awsClient, googleClient)
	log.WithField("sync_method", cfg.SyncMethod).Info("Starting synchronization")
   if cfg.SyncMethod == config.DefaultSyncMethod {
	log.Info("Using default synchronization method")
	err = c.SyncGroupsUsers(cfg.GroupMatch)
   if err != nil {
	log.WithError(err).Error("Error synchronizing groups and users")
   return err
	}
	} else {
	log.Info("Using alternative synchronization method")
	err = c.SyncUsers(cfg.UserMatch)
   if err != nil {
	log.WithError(err).Error("Error synchronizing users")
   return err
	}
	err = c.SyncGroups(cfg.GroupMatch)
   if err != nil {
	log.WithError(err).Error("Error synchronizing groups")
   return err
	}
	}
	log.Info("Synchronization completed successfully")
   return nil
   }

func (s *syncGSuite) ignoreUser(name string) bool {
	for _, u := range s.cfg.IgnoreUsers {
		if u == name {
			return true
		}
	}

	return false
}

func (s *syncGSuite) ignoreGroup(name string) bool {
	for _, g := range s.cfg.IgnoreGroups {
		if g == name {
			return true
		}
	}

	return false
}

func (s *syncGSuite) includeGroup(name string) bool {
	for _, g := range s.cfg.IncludeGroups {
		if g == name {
			return true
		}
	}

	return false
}
