/*
* FinTP - Financial Transactions Processing Application
* Copyright (C) 2013 Business Information Systems (Allevo) S.R.L.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>
* or contact Allevo at : 031281 Bucuresti, 23C Calea Vitan, Romania,
* phone +40212554577, office@allevo.ro <mailto:office@allevo.ro>, www.allevo.ro.
*/

#ifndef DBPUBLISHER_H
#define DBPUBLISHER_H

#include "MQ/MqWatcher.h"
#include "DB/DbWatcher.h"
#include "Database.h"
#include "DatabaseProvider.h"
#include "DB/DbDad.h"
#include "XPathHelper.h"

#include "CacheManager.h"

#include "../Endpoint.h"

// holds information about conenctors and duplicate detection settings
class ExportedTestObject ConnectorsDDInfo
{
	private:

		map< string, string > m_Services;
		bool m_Active;
		
	public:
		
		ConnectorsDDInfo() : m_Active( false ) {};
		
		void GetDuplicateServices( DatabaseProviderFactory *databaseProvider, const string& databaseName, const string& user, const string& password );

		bool IsDDActive( const string& connectorName ) 
		{
			if ( !m_Active ) 
				return false;
			else
			{
				if ( m_Services.find( connectorName ) == m_Services.end() )
					return false;
				return ( m_Services[ connectorName ].length() > 0 );
			}
		}

		const string Hash( const string& connectorName, const string& payload );
};

//Mq to Database
class ExportedTestObject DbPublisher : public Endpoint
{
	protected :

		// methods for controlling the endpoint 
		void internalStart();
		void internalStop();

	public:
	
		// constructor
		DbPublisher();
	
		//destructor
		~DbPublisher();

		// called before start. allows the endpoint to prepare environment
		void Init();
		
		// methods for transactional execution
		/// <summary>Make an engagement whether it can/can't process the data</summary>
		/// <returns>True if it can process the data</returns>
		//Connect to database
		string Prepare();

		/// <summary>Commits the work</summary>
		void Commit();

		/// <summary>Aborts the work and rolls back the data</summary>
		void Abort();

		/// <summary>Rollback</summary>
		void Rollback();
		
		/// <summary>Processes the data</summary>
		/// <returns>True if everything worked fine</returns>
		void Process( const string& correlationId );
		pthread_t getWatcherThreadId() { return m_Watcher.getThreadId(); }

		bool moreMessages() const { return !m_IsLast; }

	private:
		
		ConnectorsDDInfo m_DDSettings;

		MqWatcher m_Watcher;						// MQSeries watcher

		string m_WatchQueue;				
		string m_WatchQueueManager;
		string m_WatchTransportURI;
		bool m_WMQBatchFilter;

		string m_CfgDatabaseName;
		string m_CfgUserName;
		string m_CfgUserPassword;

		string m_BlobLocator;				// simplified xpath to locate the blob in the payload ( ex. /root/IDIMAGINE )
		string m_BlobFilePattern;			// folder+filename containing {xpats}s ( ex : c:\blobs\{/root/BATCHID}_{/root/IMAGEREF}.tiff )
		bool m_InsertBlob;
		
		string m_DatabaseProvider;
		string m_DatabaseName;				// QPI Database name
		string m_TableName;					// QPI Table name
		string m_RepliesTableName;			// QPI Table name
		string m_UserName;					// QPI database user
		string m_UserPassword;				// QPI user password
		
		DbDad::DadOptions m_DadOptions;		// Dad options : choose if we use a dad, if we generate the param vectors or insert with values statement
		string m_AckDadFileName;			// Ack dad, file describe how to put ack
		string m_SPinsertXmlData;			// Stored procedure that insert XML into QPI tables
		
		string m_CurrentMessageId;
		string m_CurrentGroupId;
		unsigned long m_CurrentMessageLength;
		
		CacheManager< string, string > m_DadCache;

		string getDad( const string& dadFilename );
		
		Database *m_CurrentDatabase;
		DatabaseProviderFactory *m_CurrentProvider;
		DbDad *m_Dad;

		int m_PrevBatchItem;
		string m_LastRequestor;

		bool m_BatchChanged;
		bool m_TransactionStarted;

		void UploadMessage( const string& theDadFileName, const string& xmlData, const string& xmlTable, const string& hash );
		string SaveBlobToFile( const string& xmlData );
};

#endif
