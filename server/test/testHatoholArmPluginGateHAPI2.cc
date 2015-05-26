/*
 * Copyright (C) 2015 Project Hatohol
 *
 * This file is part of Hatohol.
 *
 * Hatohol is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License, version 3
 * as published by the Free Software Foundation.
 *
 * Hatohol is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with Hatohol. If not, see
 * <http://www.gnu.org/licenses/>.
 */

#include <gcutter.h>
#include <Hatohol.h>
#include <HatoholArmPluginGateHAPI2.h>
#include "Helpers.h"
#include "DBTablesTest.h"

#include <StringUtils.h>

using namespace std;
using namespace mlpl;

namespace testHAPI2ParseTimeStamp {

void test_fullFormat(void) {
	timespec timeStamp;
	// UTC: 2015-05017 16:00:00
	// JST: 2015-05018 01:00:00
	bool succeeded =
	  HatoholArmPluginGateHAPI2::parseTimeStamp("20150517160000.123456789",
						    timeStamp);
	cppcut_assert_equal(true, succeeded);
	cppcut_assert_equal(static_cast<time_t>(1431878400),
			    timeStamp.tv_sec);
	cppcut_assert_equal(static_cast<long>(123456789),
			    timeStamp.tv_nsec);
}

void test_noNanoSecondField(void) {
	timespec timeStamp;
	bool succeeded =
	  HatoholArmPluginGateHAPI2::parseTimeStamp("20150517160000",
						    timeStamp);
	cppcut_assert_equal(true, succeeded);
	cppcut_assert_equal(static_cast<time_t>(1431878400),
			    timeStamp.tv_sec);
	cppcut_assert_equal(static_cast<long>(0),
			    timeStamp.tv_nsec);
}

void test_shortNanoSecondField(void) {
	timespec timeStamp;
	bool succeeded =
	  HatoholArmPluginGateHAPI2::parseTimeStamp("20150517160000.12",
						    timeStamp);
	cppcut_assert_equal(true, succeeded);
	cppcut_assert_equal(static_cast<time_t>(1431878400),
			    timeStamp.tv_sec);
	cppcut_assert_equal(static_cast<long>(120000000),
			    timeStamp.tv_nsec);
}

void test_nanoSecondFieldWithLeadingZero(void) {
	timespec timeStamp;
	bool succeeded =
	  HatoholArmPluginGateHAPI2::parseTimeStamp("20150517160000.012",
						    timeStamp);
	cppcut_assert_equal(true, succeeded);
	cppcut_assert_equal(static_cast<time_t>(1431878400),
			    timeStamp.tv_sec);
	cppcut_assert_equal(static_cast<long>(12000000),
			    timeStamp.tv_nsec);
}

void test_emptyTimeStamp(void) {
	timespec timeStamp;
	bool succeeded =
	  HatoholArmPluginGateHAPI2::parseTimeStamp(string(), timeStamp);
	cppcut_assert_equal(false, succeeded);
	cppcut_assert_equal(static_cast<time_t>(0),
			    timeStamp.tv_sec);
	cppcut_assert_equal(static_cast<long>(0),
			    timeStamp.tv_nsec);
}

void test_invalidDateTime(void) {
	timespec timeStamp;
	bool succeeded =
	  HatoholArmPluginGateHAPI2::parseTimeStamp(
	    "2015-05-17 16:00:00.123456789", timeStamp);
	cppcut_assert_equal(false, succeeded);
	cppcut_assert_equal((time_t) 0,
			    timeStamp.tv_sec);
	cppcut_assert_equal((long) 0,
			    timeStamp.tv_nsec);
}

void test_invalidNanoSecond(void) {
	timespec timeStamp;
	bool succeeded =
	  HatoholArmPluginGateHAPI2::parseTimeStamp(
	    "20150517160000.1234a6789", timeStamp);
	cppcut_assert_equal(false, succeeded);
	cppcut_assert_equal((time_t) 1431878400,
			    timeStamp.tv_sec);
	cppcut_assert_equal((long) 0,
			    timeStamp.tv_nsec);
}

void test_tooLongNanoSecond(void) {
	timespec timeStamp;
	bool succeeded =
	  HatoholArmPluginGateHAPI2::parseTimeStamp(
	    "20150517160000.1234567890", timeStamp);
	cppcut_assert_equal(false, succeeded);
	cppcut_assert_equal((time_t) 1431878400,
			    timeStamp.tv_sec);
	cppcut_assert_equal((long) 0,
			    timeStamp.tv_nsec);
}

}

namespace testHatoholArmPluginGateHAPI2 {

void cut_setup(void)
{
	hatoholInit();
	setupTestDB();
	loadTestDBServer();
}

void cut_teardown(void)
{
}

void test_new(void)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	cut_assert_not_null(gate);
}

void test_procedureHandlerExchangeProfile(void)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params = "";
	std::string actual =
		gate->procedureHandlerExchangeProfile(HAPI2_EXCHANGE_PROFILE, params);
	std::string expected =
		"{\"jsonrpc\":\"2.0\",\"result\":{\"procedures\":"
		  "[\"getMonitoringServerInfo\",\"getLastInfo\",\"putItems\","
		  "\"putHistory\",\"updateHosts\",\"updateHostGroups\","
		  "\"updateHostGroupMembership\",\"updateTriggers\","
		  "\"updateEvents\",\"updateHostParent\",\"updateArmInfo\""
		"]},\"name\":\"exampleName\",\"id\":1}";
	cppcut_assert_equal(expected, actual);
}

void test_procedureHandlerMonitoringServerInfo(void)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	serverInfo = testServerInfo[6];
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params = "";
	std::string actual = gate->procedureHandlerMonitoringServerInfo(
	  HAPI2_MONITORING_SERVER_INFO, params);
	std::string expected =
		"{\"jsonrpc\":\"2.0\",\"result\":{"
		 "\"serverId\":301,\"url\":\"http://10.0.0.32/nagios3\","
		 "\"type\":\"902d955c-d1f7-11e4-80f9-d43d7e3146fb\","
		 "\"nickName\":\"Akira\",\"userName\":\"nagios-operator\","
		 "\"password\":\"5t64k-f3-ui.l76n\",\"pollingIntervalSec\":300,"
		 "\"retryIntervalSec\":60,\"extendedInfo\":\"exampleExtraInfo\""
		"},\"id\":1}";
	cppcut_assert_equal(expected, actual);
}

void data_procedureHandlerLastInfo(void)
{
	gcut_add_datum("host",
	               "params", G_TYPE_STRING, "host",
	               "value", G_TYPE_STRING, "1431232440", NULL);
	gcut_add_datum("hostGroup",
	               "params", G_TYPE_STRING, "hostGroup",
	               "value", G_TYPE_STRING, "1431221640", NULL);
	gcut_add_datum("hostGroupMembership",
	               "params", G_TYPE_STRING, "hostGroupMembership",
	               "value", G_TYPE_STRING, "1431567240", NULL);
	gcut_add_datum("trigger",
	               "params", G_TYPE_STRING, "trigger",
	               "value", G_TYPE_STRING, "1431671640", NULL);
	gcut_add_datum("event",
	               "params", G_TYPE_STRING, "event",
	               "value", G_TYPE_STRING, "1431585240", NULL);
	gcut_add_datum("hostParent",
	               "params", G_TYPE_STRING, "hostParent",
	               "value", G_TYPE_STRING, "1431930840", NULL);
}

void test_procedureHandlerLastInfo(gconstpointer data)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	loadTestDBLastInfo();
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params =
	  StringUtils::sprintf("{\"jsonrpc\":\"2.0\", \"method\":\"getLastInfo\","
			       " \"params\":\"%s\", \"id\":1}",
			       gcut_data_get_string(data, "params"));
	std::string actual = gate->procedureHandlerLastInfo(
	  HAPI2_LAST_INFO, params);
	std::string expected =
	  StringUtils::sprintf("{\"jsonrpc\":\"2.0\",\"result\":\"%s\",\"id\":1}",
			       gcut_data_get_string(data, "value"));
	cppcut_assert_equal(expected, actual);
}

void test_procedureHandlerPutItems(void)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params =
		"{\"jsonrpc\":\"2.0\",\"method\":\"putItems\","
		" \"params\":{\"items\":[{\"itemId\":\"1\", \"hostId\":\"1\","
		" \"brief\":\"example brief\", \"lastValueTime\":\"20150410175523\","
		" \"lastValue\":\"example value\","
		" \"itemGroupName\":\"example name\", \"unit\":\"example unit\"},"
		" {\"itemId\":\"2\", \"hostId\":\"1\","
		" \"brief\":\"example brief\", \"lastValueTime\":\"20150410175531\","
		" \"lastValue\":\"example value\","
		" \"itemGroupName\":\"example name\", \"unit\":\"example unit\"}],"
		" \"fetchId\":\"1\"}, \"id\":1}";
	std::string actual = gate->procedureHandlerPutItems(
	  HAPI2_PUT_ITEMS, params);
	std::string expected =
		"{\"jsonrpc\":\"2.0\",\"result\":\"\",\"id\":1}";
	cppcut_assert_equal(expected, actual);
	// TODO: add DB assertion
}

void test_procedureHandlerPutHistory(void)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params =
		"{\"jsonrpc\":\"2.0\", \"method\":\"putHistory\","
		" \"params\":{\"itemId\":\"1\","
		" \"histories\":[{\"value\":\"exampleValue\","
		" \"time\":\"20150323113032.000000000\"},"
		"{\"value\":\"exampleValue2\",\"time\":\"20150323113033.000000000\"}],"
		" \"fetchId\":\"1\"}, \"id\":1}";
	std::string actual = gate->procedureHandlerPutHistory(
	  HAPI2_PUT_HISTORY, params);
	std::string expected =
		"{\"jsonrpc\":\"2.0\",\"result\":\"\",\"id\":1}";
	cppcut_assert_equal(expected, actual);
}

void test_procedureHandlerUpdateHosts(void)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params =
		"{\"jsonrpc\":\"2.0\",\"method\":\"updateHosts\", \"params\":"
		"{\"hosts\":[{\"hostId\":\"1\", \"hostName\":\"exampleHostName1\"}],"
		" \"updateType\":\"UPDATE\",\"lastInfo\":\"201504091052\"}, \"id\":1}";
	std::string actual = gate->procedureHandlerUpdateHosts(
	  HAPI2_UPDATE_HOSTS, params);
	std::string expected =
		"{\"jsonrpc\":\"2.0\",\"result\":\"SUCCESS\",\"id\":1}";
	cppcut_assert_equal(expected, actual);
}

void test_procedureHandlerUpdateHostGroups(void)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params =
		"{\"jsonrpc\":\"2.0\",\"method\":\"updateHostGroups\","
		" \"params\":{\"hostGroups\":[{\"groupId\":\"1\","
		" \"groupName\":\"Group2\"}],\"updateType\":\"ALL\","
		" \"lastInfo\":\"20150409104900\"}, \"id\":1}";
	std::string actual = gate->procedureHandlerUpdateHostGroups(
	  HAPI2_UPDATE_HOST_GROUPS, params);
	std::string expected =
		"{\"jsonrpc\":\"2.0\",\"result\":\"SUCCESS\",\"id\":1}";
	cppcut_assert_equal(expected, actual);
}

void test_procedureHandlerUpdateHostGroupMembership(void)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params =
		"{\"jsonrpc\":\"2.0\",\"method\":\"updateHostGroupMembership\","
		" \"params\":{\"hostGroupsMembership\":[{\"hostId\":\"1\","
		" \"groupIds\":[\"1\", \"2\", \"5\"]}],"
		" \"lastInfo\":\"20150409105600\", \"updateType\":\"ALL\"},"
		" \"id\":1}";
	std::string actual = gate->procedureHandlerUpdateHostGroupMembership(
	  HAPI2_UPDATE_HOST_GROUP_MEMEBRSHIP, params);
	std::string expected =
		"{\"jsonrpc\":\"2.0\",\"result\":\"SUCCESS\",\"id\":1}";
	cppcut_assert_equal(expected, actual);
}

void test_procedureHandlerUpdateTriggers(void)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params =
		"{\"jsonrpc\":\"2.0\", \"method\":\"updateTriggers\","
		" \"params\":{\"updateType\":\"UPDATED\","
		" \"lastInfo\":\"201504061606\", \"fetchId\":\"1\","
		" \"triggers\":[{\"triggerId\":\"1\", \"status\":\"OK\","
		" \"severity\":\"INFO\",\"lastChangeTime\":\"20150323175800\","
		" \"hostId\":\"1\", \"hostName\":\"exampleName\","
		" \"brief\":\"example brief\","
		" \"extendedInfo\": \"sample extended info\"}]},\"id\":1}";
	std::string actual = gate->procedureHandlerUpdateTriggers(
	  HAPI2_UPDATE_EVENTS, params);
	std::string expected =
		"{\"jsonrpc\":\"2.0\",\"result\":\"SUCCESS\",\"id\":1}";
	cppcut_assert_equal(expected, actual);
}

void test_procedureHandlerUpdateEvents(void)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params =
		"{\"jsonrpc\":\"2.0\", \"method\":\"updateEvents\","
		" \"params\":{\"events\":[{\"eventId\":\"1\","
		" \"time\":\"20150323151300\", \"type\":\"GOOD\","
		" \"triggerId\":2, \"status\": \"OK\",\"severity\":\"INFO\","
		" \"hostId\":3, \"hostName\":\"exampleName\","
		" \"brief\":\"example brief\","
		" \"extendedInfo\": \"sampel extended info\"}],"
		" \"lastInfo\":\"20150401175900\", \"fetchId\":\"1\"},\"id\":1}";
	std::string actual = gate->procedureHandlerUpdateEvents(
	  HAPI2_UPDATE_EVENTS, params);
	std::string expected =
		"{\"jsonrpc\":\"2.0\",\"result\":\"SUCCESS\",\"id\":1}";
	cppcut_assert_equal(expected, actual);
}

void test_procedureHandlerUpdateHostParents(void)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params =
		"{\"jsonrpc\":\"2.0\", \"method\":\"updateHostParent\","
		" \"params\":{\"hostParents\":"
		" [{\"childHostId\":\"12\",\"parentHostId\":\"10\"},"
		" {\"childHostId\":\"11\",\"parentHostId\":\"20\"}],"
		" \"updateType\":\"ALL\", \"lastInfo\":\"201504152246\"}, \"id\":1}";
	std::string actual = gate->procedureHandlerUpdateHostParents(
	  HAPI2_UPDATE_HOST_PARENTS, params);
	std::string expected =
		"{\"jsonrpc\":\"2.0\",\"result\":\"SUCCESS\",\"id\":1}";
	cppcut_assert_equal(expected, actual);
}

void test_procedureHandlerUpdateArmInfo(void)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params =
		"{\"jsonrpc\":\"2.0\", \"method\":\"updateArmInfo\","
		" \"params\":{\"lastStatus\":\"INIT\","
		" \"failureReason\":\"Example reason\","
		" \"lastSuccessTime\":\"20150313161100\","
		" \"lastFailureTime\":\"20150313161530\","
		" \"numSuccess\":165, \"numFailure\":10}, \"id\":1}";
	std::string actual = gate->procedureHandlerUpdateArmInfo(
	  HAPI2_UPDATE_ARM_INFO, params);
	std::string expected =
		"{\"jsonrpc\":\"2.0\",\"result\":\"SUCCESS\",\"id\":1}";
	cppcut_assert_equal(expected, actual);
}
}
