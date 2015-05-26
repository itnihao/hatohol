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

#include <StringUtils.h>
#include <JSONParser.h>
#include <HatoholArmPluginInterfaceHAPI2.h>
#include "HatoholArmPluginGateHAPI2.h"
#include "ThreadLocalDBCache.h"
#include "UnifiedDataStore.h"
#include "ArmFake.h"

using namespace std;
using namespace mlpl;

std::list<HAPI2ProcedureDef> defaultValidProcedureList = {
	{PROCEDURE_BOTH,   "exchangeProfile",           SERVER_MANDATORY_HAP_OPTIONAL},
	{PROCEDURE_SERVER, "getMonitoringServerInfo",   SERVER_MANDATORY},
	{PROCEDURE_SERVER, "getLastInfo",               SERVER_MANDATORY},
	{PROCEDURE_SERVER, "putItems",                  SERVER_OPTIONAL},
	{PROCEDURE_SERVER, "putHistory",                SERVER_OPTIONAL},
	{PROCEDURE_SERVER, "updateHosts",               SERVER_OPTIONAL},
	{PROCEDURE_SERVER, "updateHostGroups",          SERVER_OPTIONAL},
	{PROCEDURE_SERVER, "updateHostGroupMembership", SERVER_OPTIONAL},
	{PROCEDURE_SERVER, "updateTriggers",            SERVER_OPTIONAL},
	{PROCEDURE_SERVER, "updateEvents",              SERVER_OPTIONAL},
	{PROCEDURE_SERVER, "updateHostParent",          SERVER_OPTIONAL},
	{PROCEDURE_SERVER, "updateArmInfo",             SERVER_MANDATORY},
	{PROCEDURE_HAP,    "fetchItems",                HAP_OPTIONAL},
	{PROCEDURE_HAP,    "fetchHistory",              HAP_OPTIONAL},
	{PROCEDURE_HAP,    "fetchTriggers",             HAP_OPTIONAL},
	{PROCEDURE_HAP,    "fetchEvents",               HAP_OPTIONAL},
};

struct HatoholArmPluginGateHAPI2::Impl
{
	// We have a copy. The access to the object is MT-safe.
	const MonitoringServerInfo m_serverInfo;
	ArmFake m_armFake;
	ArmStatus m_armStatus;

	Impl(const MonitoringServerInfo &_serverInfo,
	     HatoholArmPluginGateHAPI2 *hapghapi)
	: m_serverInfo(_serverInfo),
	  m_armFake(m_serverInfo),
	  m_armStatus()
	{
		ThreadLocalDBCache cache;
		DBTablesConfig &dbConfig = cache.getConfig();
		const ServerIdType &serverId = m_serverInfo.id;
		ArmPluginInfo armPluginInfo;
		if (!dbConfig.getArmPluginInfo(armPluginInfo, serverId)) {
			MLPL_ERR("Failed to get ArmPluginInfo: serverId: %d\n",
				 serverId);
			return;
		}
		hapghapi->setArmPluginInfo(armPluginInfo);
	}

	~Impl()
	{
		m_armStatus.setRunningStatus(false);
	}

	void start(void)
	{
		m_armStatus.setRunningStatus(true);
	}
};

// ---------------------------------------------------------------------------
// Public methods
// ---------------------------------------------------------------------------
HatoholArmPluginGateHAPI2::HatoholArmPluginGateHAPI2(
  const MonitoringServerInfo &serverInfo, const bool &autoStart)
: m_impl(new Impl(serverInfo, this))
{
	registerProcedureHandler(
	  HAPI2_EXCHANGE_PROFILE,
	  (ProcedureHandler)
	    &HatoholArmPluginGateHAPI2::procedureHandlerExchangeProfile);
	registerProcedureHandler(
	  HAPI2_MONITORING_SERVER_INFO,
	  (ProcedureHandler)
	    &HatoholArmPluginGateHAPI2::procedureHandlerMonitoringServerInfo);
	registerProcedureHandler(
	  HAPI2_LAST_INFO,
	  (ProcedureHandler)
	    &HatoholArmPluginGateHAPI2::procedureHandlerLastInfo);
	registerProcedureHandler(
	  HAPI2_PUT_ITEMS,
	  (ProcedureHandler)
	    &HatoholArmPluginGateHAPI2::procedureHandlerPutItems);
	registerProcedureHandler(
	  HAPI2_PUT_HISTORY,
	  (ProcedureHandler)
	    &HatoholArmPluginGateHAPI2::procedureHandlerPutHistory);
	registerProcedureHandler(
	  HAPI2_UPDATE_HOSTS,
	  (ProcedureHandler)
	    &HatoholArmPluginGateHAPI2::procedureHandlerUpdateHosts);
	registerProcedureHandler(
	  HAPI2_UPDATE_HOST_GROUPS,
	  (ProcedureHandler)
	    &HatoholArmPluginGateHAPI2::procedureHandlerUpdateHostGroups);
	registerProcedureHandler(
	  HAPI2_UPDATE_HOST_GROUP_MEMEBRSHIP,
	  (ProcedureHandler)
	    &HatoholArmPluginGateHAPI2::procedureHandlerUpdateHostGroupMembership);
	registerProcedureHandler(
	  HAPI2_UPDATE_TRIGGERS,
	  (ProcedureHandler)
	    &HatoholArmPluginGateHAPI2::procedureHandlerUpdateTriggers);
	registerProcedureHandler(
	  HAPI2_UPDATE_EVENTS,
	  (ProcedureHandler)
	    &HatoholArmPluginGateHAPI2::procedureHandlerUpdateEvents);
	registerProcedureHandler(
	  HAPI2_UPDATE_ARM_INFO,
	  (ProcedureHandler)
	    &HatoholArmPluginGateHAPI2::procedureHandlerUpdateArmInfo);

	if (autoStart)
		m_impl->start();
}

void HatoholArmPluginGateHAPI2::start(void)
{
	HatoholArmPluginInterfaceHAPI2::start();
	m_impl->start();
}

bool HatoholArmPluginGateHAPI2::parseTimeStamp(
  const string &timeStampString, timespec &timeStamp)
{
	timeStamp.tv_sec = 0;
	timeStamp.tv_nsec = 0;

	StringVector list;
	StringUtils::split(list, timeStampString, '.', false);

	if (list.empty() || list.size() > 2)
		goto ERR;
	struct tm tm;
	if (!strptime(list[0].c_str(), "%4Y%2m%2d%2H%2M%2S", &tm))
		goto ERR;
	timeStamp.tv_sec = timegm(&tm); // as UTC

	if (list.size() == 1)
		return true;

	if (list[1].size() > 9)
		goto ERR;
	for (size_t i = 0; i < list[1].size(); i++) {
		unsigned int ch = list[1][i];
		if (ch < '0' || ch > '9')
			goto ERR;
	}
	for (size_t i = list[1].size(); i < 9; i++)
		list[1] += '0';
	timeStamp.tv_nsec = atol(list[1].c_str());

	return true;

 ERR:
	MLPL_ERR("Invalid timestamp format: %s\n",
		 timeStampString.c_str());
	return false;
}

static bool parseTimeStamp(
  JSONParser &parser, const string &member, timespec &timeStamp)
{
	string timeStampString;
	parser.read(member, timeStampString);
	return HatoholArmPluginGateHAPI2::parseTimeStamp(timeStampString,
							 timeStamp);
}

// ---------------------------------------------------------------------------
// Protected methods
// ---------------------------------------------------------------------------
HatoholArmPluginGateHAPI2::~HatoholArmPluginGateHAPI2()
{
}

const MonitoringServerInfo &
HatoholArmPluginGateHAPI2::getMonitoringServerInfo(void) const
{
	return m_impl->m_armFake.getServerInfo();
}

const ArmStatus &HatoholArmPluginGateHAPI2::getArmStatus(void) const
{
	return m_impl->m_armStatus;
}

string HatoholArmPluginGateHAPI2::procedureHandlerExchangeProfile(
  const HAPI2ProcedureType type, const string &params)
{
	JSONBuilder agent;
	agent.startObject();
	agent.add("jsonrpc", "2.0");
	agent.startObject("result");
	agent.startArray("procedures");
	for (auto defaultValidProcedureDef : defaultValidProcedureList) {
		if (defaultValidProcedureDef.type == PROCEDURE_BOTH ||
		    defaultValidProcedureDef.type == PROCEDURE_HAP)
			continue;
		agent.add(defaultValidProcedureDef.name);
	}
	agent.endArray(); // procedures
	agent.endObject(); // result
	agent.add("name", "exampleName"); // TODO: add process name mechanism
	agent.add("id", 1);
	agent.endObject();
	return agent.generate();
}

string HatoholArmPluginGateHAPI2::procedureHandlerMonitoringServerInfo(
  const HAPI2ProcedureType type, const string &params)
{
	JSONBuilder agent;
	agent.startObject();
	agent.add("jsonrpc", "2.0");
	agent.startObject("result");
	const MonitoringServerInfo &serverInfo = m_impl->m_serverInfo;
	agent.add("serverId", serverInfo.id);
	agent.add("url", serverInfo.baseURL);
	agent.add("type", serverInfo.type);
	agent.add("nickName", serverInfo.nickname);
	agent.add("userName", serverInfo.userName);
	agent.add("password", serverInfo.password);
	agent.add("pollingIntervalSec", serverInfo.pollingIntervalSec);
	agent.add("retryIntervalSec", serverInfo.retryIntervalSec);
	// TODO: Use serverInfo.extendedInfo
	agent.add("extendedInfo", "exampleExtraInfo");
	agent.endObject(); // result
	agent.add("id", 1);
	agent.endObject();
	return agent.generate();
}

string HatoholArmPluginGateHAPI2::procedureHandlerLastInfo(
  const HAPI2ProcedureType type, const string &params)
{
	UnifiedDataStore *dataStore = UnifiedDataStore::getInstance();
	TriggerInfoList triggerInfoList;
	TriggersQueryOption triggersQueryOption(USER_ID_SYSTEM);
	dataStore->getTriggerList(triggerInfoList, triggersQueryOption);
	TriggerInfoListIterator it = triggerInfoList.begin();
	TriggerInfo &firstTriggerInfo = *it;

	JSONBuilder agent;
	agent.startObject();
	agent.add("jsonrpc", "2.0");
	agent.add("result", firstTriggerInfo.lastChangeTime.tv_sec);
	agent.add("id", 1);
	agent.endObject();
	return agent.generate();
}

static bool parseItemParams(JSONParser &parser, ItemInfoList &itemInfoList)
{
	parser.startObject("params");
	parser.startObject("items");
	size_t num = parser.countElements();

	for (size_t i = 0; i < num; i++) {
		if (!parser.startElement(i)) {
			MLPL_ERR("Failed to parse item contents.\n");
			return false;
		}

		ItemInfo itemInfo;
		parser.read("itemId", itemInfo.id);
		parser.read("hostId", itemInfo.hostIdInServer);
		parser.read("brief", itemInfo.brief);
		parseTimeStamp(parser, "lastValueTime", itemInfo.lastValueTime);
		parser.read("lastValue", itemInfo.lastValue);
		parser.read("itemGroupName", itemInfo.itemGroupName);
		parser.read("unit", itemInfo.unit);
		parser.endElement();

		itemInfoList.push_back(itemInfo);
	}
	parser.endObject(); // items
	parser.endObject(); // params
	return true;
};

string HatoholArmPluginGateHAPI2::procedureHandlerPutItems(
  const HAPI2ProcedureType type, const string &params)
{
	UnifiedDataStore *dataStore = UnifiedDataStore::getInstance();
	ItemInfoList itemList;
	JSONParser parser(params);
	bool succeeded = parseItemParams(parser, itemList);
	dataStore->addItemList(itemList);

	JSONBuilder agent;
	agent.startObject();
	agent.add("jsonrpc", "2.0");
	agent.add("result", "");
	agent.add("id", 1);
	agent.endObject();
	return agent.generate();
}

static bool parseHistoryParams(JSONParser &parser, HistoryInfoVect &historyInfoVect)
{
	parser.startObject("params");
	ItemIdType itemId = "";
	parser.read("itemId", itemId);
	parser.startObject("histories");
	size_t num = parser.countElements();

	for (size_t j = 0; j < num; j++) {
		if (!parser.startElement(j)) {
			MLPL_ERR("Failed to parse histories contents.\n");
			return false;
		}

		HistoryInfo historyInfo;
		historyInfo.itemId = itemId;
		parser.read("value", historyInfo.value);
		parseTimeStamp(parser, "time", historyInfo.clock);
		parser.endElement();

		historyInfoVect.push_back(historyInfo);
	}
	parser.endObject(); // histories
	parser.endObject(); // params
	return true;
};

string HatoholArmPluginGateHAPI2::procedureHandlerPutHistory(
  const HAPI2ProcedureType type, const string &params)
{
	HistoryInfoVect historyInfoVect;
	JSONParser parser(params);
	bool succeeded = parseHistoryParams(parser, historyInfoVect);
	// TODO: Store or transport historyInfoVect

	JSONBuilder agent;
	agent.startObject();
	agent.add("jsonrpc", "2.0");
	agent.add("result", "");
	agent.add("id", 1);
	agent.endObject();
	return agent.generate();
}

static bool parseHostsParams(JSONParser &parser, ServerHostDefVect &hostInfoVect)
{
	parser.startObject("params");
	parser.startObject("hosts");
	size_t num = parser.countElements();
	for (size_t j = 0; j < num; j++) {
		if (!parser.startElement(j)) {
			MLPL_ERR("Failed to parse hosts contents.\n");
			return false;
		}

		ServerHostDef hostInfo;
		int64_t hostId;
		parser.read("hostId", hostId);
		hostInfo.hostIdInServer = hostId;
		parser.read("hostName", hostInfo.name);
		parser.endElement();

		hostInfoVect.push_back(hostInfo);
	}
	parser.endObject(); // hosts
	parser.endObject(); // params
	return true;
};

static bool parseHostsUpdateType(JSONParser &parser, string &updateType)
{
	parser.read("updateType", updateType);
	return true;
};

string HatoholArmPluginGateHAPI2::procedureHandlerUpdateHosts(
  const HAPI2ProcedureType type, const string &params)
{
	UnifiedDataStore *dataStore = UnifiedDataStore::getInstance();
	ServerHostDefVect hostInfoVect;
	JSONParser parser(params);
	bool succeeded = parseHostsParams(parser, hostInfoVect);
	string updateType;
	bool checkInvalidHosts = parseHostsUpdateType(parser, updateType);
	// TODO: implement validation for Hosts
	string lastInfo;
	if (!parser.read("lastInfo", lastInfo) ) {
		upsertLastInfo(lastInfo, LAST_INFO_HOST);
	}

	dataStore->upsertHosts(hostInfoVect);
	string result = succeeded ? "SUCCESS" : "FAILURE";

	JSONBuilder agent;
	agent.startObject();
	agent.add("jsonrpc", "2.0");
	agent.add("result", result);
	agent.add("id", 1);
	agent.endObject();
	return agent.generate();
}

static bool parseHostGroupsParams(JSONParser &parser,
				  HostgroupVect &hostgroupVect)
{
	parser.startObject("params");
	parser.startObject("hostGroups");
	size_t num = parser.countElements();
	for (size_t j = 0; j < num; j++) {
		if (!parser.startElement(j)) {
			MLPL_ERR("Failed to parse hosts contents.\n");
			return false;
		}

		Hostgroup hostgroup;
		int64_t hostIdInServer;
		parser.read("groupId", hostIdInServer);
		hostgroup.idInServer = hostIdInServer;
		parser.read("groupName", hostgroup.name);
		parser.endElement();

		hostgroupVect.push_back(hostgroup);
	}
	parser.endObject(); // hosts
	parser.endObject(); // params
	return true;
};

static bool parseHostGroupsUpdateType(JSONParser &parser, string &updateType)
{
	parser.read("updateType", updateType);
	return true;
};

string HatoholArmPluginGateHAPI2::procedureHandlerUpdateHostGroups(
  const HAPI2ProcedureType type, const string &params)
{
	UnifiedDataStore *dataStore = UnifiedDataStore::getInstance();
	HostgroupVect hostgroupVect;
	JSONParser parser(params);
	bool succeeded = parseHostGroupsParams(parser, hostgroupVect);
	string result = succeeded ? "SUCCESS" : "FAILURE";

	string updateType;
	bool checkInvalidHostGroups = parseHostGroupsUpdateType(parser, updateType);
	// TODO: implement validation for HostGroups
	string lastInfo;
	if (!parser.read("lastInfo", lastInfo) ) {
		upsertLastInfo(lastInfo, LAST_INFO_HOST_GROUP);
	}
	dataStore->upsertHostgroups(hostgroupVect);

	JSONBuilder agent;
	agent.startObject();
	agent.add("jsonrpc", "2.0");
	agent.add("result", result);
	agent.add("id", 1);
	agent.endObject();
	// TODO: implement replying exchange profile procedure with AMQP
	return agent.generate();
}


static bool parseHostGroupMembershipParams(
  JSONParser &parser,
  HostgroupMemberVect &hostgroupMemberVect)
{
	parser.startObject("params");
	parser.startObject("hostGroupsMembership");
	size_t num = parser.countElements();
	for (size_t i = 0; i < num; i++) {
		if (!parser.startElement(i)) {
			MLPL_ERR("Failed to parse hosts contents.\n");
			return false;
		}

		HostgroupMember hostgroupMember;
		string hostId;
		parser.read("hostId", hostId);
		hostgroupMember.hostId = StringUtils::toUint64(hostId);
		parser.startObject("groupIds");
		size_t groupIdNum = parser.countElements();
		for (size_t j = 0; j < groupIdNum; j++) {
			parser.read(j, hostgroupMember.hostgroupIdInServer);
			hostgroupMemberVect.push_back(hostgroupMember);
		}
		parser.endElement();
	}
	parser.endObject(); // hosts
	parser.endObject(); // params
	return true;
};

string HatoholArmPluginGateHAPI2::procedureHandlerUpdateHostGroupMembership(
  const HAPI2ProcedureType type, const string &params)
{
	UnifiedDataStore *dataStore = UnifiedDataStore::getInstance();
	HostgroupMemberVect hostgroupMembershipVect;
	JSONParser parser(params);
	bool succeeded = parseHostGroupMembershipParams(parser, hostgroupMembershipVect);
	string result = succeeded ? "SUCCESS" : "FAILURE";

	string updateType;
	bool checkInvalidHostGroupMembership =
	  parseHostGroupsUpdateType(parser, updateType);
	// TODO: implement validation for HostGroupMembership
	string lastInfo;
	if (!parser.read("lastInfo", lastInfo) ) {
		upsertLastInfo(lastInfo, LAST_INFO_HOST_GROUP_MEMBERSHIP);
	}
	dataStore->upsertHostgroupMembers(hostgroupMembershipVect);

	JSONBuilder agent;
	agent.startObject();
	agent.add("jsonrpc", "2.0");
	agent.add("result", result);
	agent.add("id", 1);
	agent.endObject();
	// TODO: implement replying exchange profile procedure with AMQP
	return agent.generate();
}

static bool parseTriggersParams(JSONParser &parser, TriggerInfoList &triggerInfoList)
{
	parser.startObject("params");
	parser.startObject("triggers");
	size_t num = parser.countElements();

	for (size_t i = 0; i < num; i++) {
		if (!parser.startElement(i)) {
			MLPL_ERR("Failed to parse event contents.\n");
			return false;
		}

		TriggerInfo triggerInfo;
		parser.read("triggerId", triggerInfo.id);
		string status;
		parser.read("status",    status);
		if (status == "OK")
			triggerInfo.status = TRIGGER_STATUS_OK;
		else if (status == "NG")
			triggerInfo.status = TRIGGER_STATUS_PROBLEM;
		else
			triggerInfo.status = TRIGGER_STATUS_UNKNOWN;

		string severity;
		parser.read("severity", severity);
		if (severity == "ALL")
			triggerInfo.severity = TRIGGER_SEVERITY_ALL;
		else if (severity == "UNKNOWN")
			triggerInfo.severity = TRIGGER_SEVERITY_UNKNOWN;
		else if (severity == "INFO")
			triggerInfo.severity = TRIGGER_SEVERITY_INFO;
		else if (severity == "WARNING")
			triggerInfo.severity = TRIGGER_SEVERITY_WARNING;
		else if (severity == "ERROR")
			triggerInfo.severity = TRIGGER_SEVERITY_ERROR;
		else if (severity == "CRITICAL")
			triggerInfo.severity = TRIGGER_SEVERITY_CRITICAL;
		else if (severity == "EMERGENCY")
			triggerInfo.severity = TRIGGER_SEVERITY_EMERGENCY;
		string lastChangeTime;
		parseTimeStamp(parser, "lastChangeTime", triggerInfo.lastChangeTime);
		parser.read("hostId",       triggerInfo.hostIdInServer);
		parser.read("hostName",     triggerInfo.hostName);
		parser.read("brief",        triggerInfo.brief);
		parser.read("extendedInfo", triggerInfo.extendedInfo);
		parser.endElement();

		triggerInfoList.push_back(triggerInfo);
	}
	parser.endObject(); // events
	parser.endObject(); // params
	return true;
};

string HatoholArmPluginGateHAPI2::procedureHandlerUpdateTriggers(
  const HAPI2ProcedureType type, const string &params)
{
	ThreadLocalDBCache cache;
	DBTablesMonitoring &dbMonitoring = cache.getMonitoring();
	TriggerInfoList triggerInfoList;
	JSONParser parser(params);
	bool succeeded = parseTriggersParams(parser, triggerInfoList);
	string result = succeeded ? "SUCCESS" : "FAILURE";
	dbMonitoring.addTriggerInfoList(triggerInfoList);

	JSONBuilder agent;
	agent.startObject();
	agent.add("jsonrpc", "2.0");
	agent.add("result", result);
	agent.add("id", 1);
	agent.endObject();
	// TODO: implement replying exchange profile procedure with AMQP
	return agent.generate();
}

static bool parseEventsParams(JSONParser &parser, EventInfoList &eventInfoList)
{
	parser.startObject("params");
	parser.startObject("events");
	size_t num = parser.countElements();
	constexpr const size_t numLimit = 1000;

	if (num > numLimit) {
		MLPL_ERR("Event Object is too large. "
			 "Excess object size limit(%zd).\n", numLimit);
		return false;
	}

	for (size_t i = 0; i < num; i++) {
		if (!parser.startElement(i)) {
			MLPL_ERR("Failed to parse event contents.\n");
			return false;
		}

		EventInfo eventInfo;
		parser.read("eventId",      eventInfo.id);
		parseTimeStamp(parser, "time", eventInfo.time);
		int64_t type, status, severity;
		parser.read("type",         type);
		eventInfo.type = (EventType)type;
		parser.read("triggerId",    eventInfo.triggerId);
		parser.read("status",       status);
		eventInfo.status = (TriggerStatusType)status;
		parser.read("severity",     severity);
		eventInfo.severity = (TriggerSeverityType)severity;
		parser.read("hostId",       eventInfo.hostIdInServer);
		parser.read("hostName",     eventInfo.hostName);
		parser.read("brief",        eventInfo.brief);
		parser.read("extendedInfo", eventInfo.extendedInfo);
		parser.endElement();

		eventInfoList.push_back(eventInfo);
	}
	parser.endObject(); // events
	parser.endObject(); // params
	return true;
};

string HatoholArmPluginGateHAPI2::procedureHandlerUpdateEvents(
  const HAPI2ProcedureType type, const string &params)
{
	UnifiedDataStore *dataStore = UnifiedDataStore::getInstance();
	EventInfoList eventInfoList;
	JSONParser parser(params);
	bool succeeded = parseEventsParams(parser, eventInfoList);
	string result = succeeded ? "SUCCESS" : "FAILURE";
	dataStore->addEventList(eventInfoList);
	string lastInfoValue;
	if (!parser.read("lastInfo", lastInfoValue) ) {
		upsertLastInfo(lastInfoValue, LAST_INFO_EVENT);
	}

	JSONBuilder agent;
	agent.startObject();
	agent.add("jsonrpc", "2.0");
	agent.add("result", result);
	agent.add("id", 1);
	agent.endObject();
	// TODO: implement replying exchange profile procedure with AMQP
	return agent.generate();
}

static bool parseArmInfoParams(JSONParser &parser, ArmInfo &armInfo)
{
	parser.startObject("params");
	string status;
	parser.read("lastStatus", status);
	if (status == "INIT")
		armInfo.stat = ARM_WORK_STAT_INIT;
	else if (status == "OK")
		armInfo.stat = ARM_WORK_STAT_OK;
	else
		armInfo.stat = ARM_WORK_STAT_FAILURE;
	parser.read("failureReason", armInfo.failureComment);
	timespec successTime, failureTime;
	parseTimeStamp(parser, "lastSuccessTime", successTime);
	parseTimeStamp(parser, "lastFailureTime", failureTime);
	SmartTime lastSuccessTime(successTime);
	SmartTime lastFailureTime(failureTime);
	armInfo.statUpdateTime = lastSuccessTime;
	armInfo.lastFailureTime = lastFailureTime;

	int64_t numSuccess, numFailure;
	parser.read("numSuccess", numSuccess);
	parser.read("numFailure", numFailure);
	armInfo.numUpdate = (size_t)numSuccess;
	armInfo.numFailure = (size_t)numFailure;

	return true;
};

string HatoholArmPluginGateHAPI2::procedureHandlerUpdateArmInfo(
  const HAPI2ProcedureType type, const string &params)
{
	ArmStatus status;
	ArmInfo armInfo;
	JSONParser parser(params);
	bool succeeded = parseArmInfoParams(parser, armInfo);
	status.setArmInfo(armInfo);
	string result = succeeded ? "SUCCESS" : "FAILURE";

	JSONBuilder agent;
	agent.startObject();
	agent.add("jsonrpc", "2.0");
	agent.add("result", result);
	agent.add("id", 1);
	agent.endObject();
	// TODO: implement replying exchange profile procedure with AMQP
	return agent.generate();
}

// ---------------------------------------------------------------------------
// Protected methods
// ---------------------------------------------------------------------------

void HatoholArmPluginGateHAPI2::upsertLastInfo(string lastInfoValue, LastInfoType type)
{
	ThreadLocalDBCache cache;
	DBTablesLastInfo &dbLastInfo = cache.getLastInfo();
	OperationPrivilege privilege(USER_ID_SYSTEM);
	const MonitoringServerInfo &serverInfo = m_impl->m_serverInfo;
	LastInfoDef lastInfo;
	lastInfo.dataType = type;
	lastInfo.value = lastInfoValue;
	lastInfo.serverId = serverInfo.id;

	LastInfoQueryOption option(USER_ID_SYSTEM);
	option.setLastInfoType(type);
	LastInfoDefList lastInfoList;
	dbLastInfo.getLastInfoList(lastInfoList, option);
	if (lastInfoList.empty())
		dbLastInfo.addLastInfo(lastInfo, privilege);
	else
		dbLastInfo.updateLastInfo(lastInfo, privilege);
}
