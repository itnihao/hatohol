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

#include <Hatohol.h>
#include <HatoholArmPluginGateHAPI2.h>
#include "Helpers.h"

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
	cppcut_assert_equal((time_t) 1431878400,
			    timeStamp.tv_sec);
	cppcut_assert_equal((long) 123456789,
			    timeStamp.tv_nsec);
}

void test_noNanoSecondField(void) {
	timespec timeStamp;
	bool succeeded =
	  HatoholArmPluginGateHAPI2::parseTimeStamp("20150517160000",
						    timeStamp);
	cppcut_assert_equal(true, succeeded);
	cppcut_assert_equal((time_t) 1431878400,
			    timeStamp.tv_sec);
	cppcut_assert_equal((long) 0,
			    timeStamp.tv_nsec);
}

void test_shortNanoSecondField(void) {
	timespec timeStamp;
	bool succeeded =
	  HatoholArmPluginGateHAPI2::parseTimeStamp("20150517160000.12",
						    timeStamp);
	cppcut_assert_equal(true, succeeded);
	cppcut_assert_equal((time_t) 1431878400,
			    timeStamp.tv_sec);
	cppcut_assert_equal((long) 120000000,
			    timeStamp.tv_nsec);
}

void test_nanoSecondFieldWithLeadingZero(void) {
	timespec timeStamp;
	bool succeeded =
	  HatoholArmPluginGateHAPI2::parseTimeStamp("20150517160000.012",
						    timeStamp);
	cppcut_assert_equal(true, succeeded);
	cppcut_assert_equal((time_t) 1431878400,
			    timeStamp.tv_sec);
	cppcut_assert_equal((long) 12000000,
			    timeStamp.tv_nsec);
}

void test_emptyTimeStamp(void) {
	timespec timeStamp;
	bool succeeded =
	  HatoholArmPluginGateHAPI2::parseTimeStamp(string(), timeStamp);
	cppcut_assert_equal(false, succeeded);
	cppcut_assert_equal((time_t) 0,
			    timeStamp.tv_sec);
	cppcut_assert_equal((long) 0,
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
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params = "";
	std::string actual = gate->procedureHandlerMonitoringServerInfo(
	  HAPI2_MONITORING_SERVER_INFO, params);
	std::string expected =
		"{\"jsonrpc\":\"2.0\",\"result\":{\"servers\":"
		 "[{\"serverId\":1,\"url\":\"\",\"type\":0,"
		   "\"nickName\":\"zabbix-vm\",\"userName\":\"Admin\","
		   "\"password\":\"zabbix\",\"pollingIntervalSec\":60,"
		   "\"retryIntervalSec\":10,\"extendedInfo\":\"exampleExtraInfo\"},"
		  "{\"serverId\":2,\"url\":\"\",\"type\":2,"
		   "\"nickName\":\"zabbix-hapi\",\"userName\":\"Admin\","
		   "\"password\":\"zabbix\",\"pollingIntervalSec\":30,"
		   "\"retryIntervalSec\":10,"
		   "\"extendedInfo\":\"exampleExtraInfo\"}]},\"id\":1}";
	cppcut_assert_equal(expected, actual);
}

void test_procedureHandlerLastInfoWithTrigger(void)
{
	MonitoringServerInfo serverInfo;
	initServerInfo(serverInfo);
	HatoholArmPluginGateHAPI2Ptr gate(
	  new HatoholArmPluginGateHAPI2(serverInfo), false);
	std::string params = "trigger";
	std::string actual = gate->procedureHandlerLastInfo(
	  HAPI2_LAST_INFO, params);
	std::string expected =
		"{\"jsonrpc\":\"2.0\",\"result\":1425025540,\"id\":1}";
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
		" \"brief\":\"example brief\", \"lastValueTime\":\"201504101755\","
		" \"lastValue\":\"example value\","
		" \"itemGroupName\":\"example name\", \"unit\":\"example unit\"},"
		" {\"itemId\":\"2\", \"hostId\":\"1\","
		" \"brief\":\"example brief\", \"lastValueTime\":\"201504101755\","
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
		" \"time\":\"201503231130\"},"
		"{\"value\":\"exampleValue2\",\"time\":\"201503231130\"}],"
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
}
