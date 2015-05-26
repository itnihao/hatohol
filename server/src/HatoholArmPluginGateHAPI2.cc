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

#include <json-glib/json-glib.h>

#include <StringUtils.h>

#include "HatoholArmPluginGateHAPI2.h"
#include "ThreadLocalDBCache.h"
#include "UnifiedDataStore.h"
#include "ArmFake.h"
#include "AMQPConsumer.h"
#include "AMQPConnectionInfo.h"
#include "AMQPMessageHandler.h"
#include "GateJSONEventMessage.h"

using namespace std;
using namespace mlpl;

class AMQPJSONMessageHandler : public AMQPMessageHandler
{
public:
	AMQPJSONMessageHandler(const MonitoringServerInfo &serverInfo)
	: m_serverInfo(serverInfo),
	  m_hosts()
	{
		initialize();
	}

	bool handle(const amqp_envelope_t *envelope)
	{
		const amqp_bytes_t *content_type =
			&(envelope->message.properties.content_type);
		// TODO: check content-type
		const amqp_bytes_t *body = &(envelope->message.body);

		MLPL_DBG("message: <%.*s>/<%.*s>\n",
			 static_cast<int>(content_type->len),
			 static_cast<char *>(content_type->bytes),
			 static_cast<int>(body->len),
			 static_cast<char *>(body->bytes));

		JsonParser *parser = json_parser_new();
		GError *error = NULL;
		if (json_parser_load_from_data(parser,
					       static_cast<char *>(body->bytes),
					       static_cast<int>(body->len),
					       &error)) {
			process(json_parser_get_root(parser));
		} else {
			g_error_free(error);
		}
		g_object_unref(parser);
		return true;
	}

private:
	MonitoringServerInfo m_serverInfo;
	map<string, HostIdType> m_hosts;

	void initialize()
	{
		// TODO: implement me!
	}

	void process(JsonNode *root)
	{
		// TODO: implement me!
	}
};

struct HatoholArmPluginGateHAPI2::Impl
{
	// We have a copy. The access to the object is MT-safe.
	const MonitoringServerInfo serverInfo;
	AMQPConnectionInfo m_connectionInfo;
	AMQPConsumer *m_consumer;
	AMQPJSONMessageHandler *m_handler;
	ArmFake m_armFake;
	ArmStatus m_armStatus;

	Impl(const MonitoringServerInfo &_serverInfo,
	     HatoholArmPluginGateHAPI2 *hapghapi)
	: serverInfo(_serverInfo),
	  m_connectionInfo(),
	  m_consumer(NULL),
	  m_handler(NULL),
	  m_armFake(serverInfo),
	  m_armStatus()
	{
		ThreadLocalDBCache cache;
		DBTablesConfig &dbConfig = cache.getConfig();
		const ServerIdType &serverId = serverInfo.id;
		ArmPluginInfo armPluginInfo;
		if (!dbConfig.getArmPluginInfo(armPluginInfo, serverId)) {
			MLPL_ERR("Failed to get ArmPluginInfo: serverId: %d\n",
				 serverId);
			return;
		}

		if (!armPluginInfo.brokerUrl.empty())
			m_connectionInfo.setURL(armPluginInfo.brokerUrl);

		string queueName;
		if (armPluginInfo.staticQueueAddress.empty())
			queueName = generateQueueName(serverInfo);
		else
			queueName = armPluginInfo.staticQueueAddress;
		m_connectionInfo.setQueueName(queueName);

		m_connectionInfo.setTLSCertificatePath(
			armPluginInfo.tlsCertificatePath);
		m_connectionInfo.setTLSKeyPath(
			armPluginInfo.tlsKeyPath);
		m_connectionInfo.setTLSCACertificatePath(
			armPluginInfo.tlsCACertificatePath);
		m_connectionInfo.setTLSVerifyEnabled(
			armPluginInfo.isTLSVerifyEnabled());

		m_handler = new AMQPJSONMessageHandler(serverInfo);
		m_consumer = new AMQPConsumer(m_connectionInfo, m_handler);
	}

	~Impl()
	{
		if (m_consumer) {
			m_consumer->exitSync();
			m_armStatus.setRunningStatus(false);
			delete m_consumer;
		}
		delete m_handler;
	}

	void start(void)
	{
		if (!m_consumer)
			return;
		m_consumer->start();
		m_armStatus.setRunningStatus(true);
	}

private:
	string generateQueueName(const MonitoringServerInfo &serverInfo)
	{
		return StringUtils::sprintf("gate.%" FMT_SERVER_ID,
					    serverInfo.id);
	}
};

// ---------------------------------------------------------------------------
// Public methods
// ---------------------------------------------------------------------------
HatoholArmPluginGateHAPI2::HatoholArmPluginGateHAPI2(
  const MonitoringServerInfo &serverInfo)
: m_impl(new Impl(serverInfo, this))
{
	// implement me!
}

// ---------------------------------------------------------------------------
// Protected methods
// ---------------------------------------------------------------------------
HatoholArmPluginGateHAPI2::~HatoholArmPluginGateHAPI2()
{
}
