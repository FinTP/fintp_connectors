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

#ifdef WIN32
	/*#ifdef _DEBUG
		//#define _CRTDBG_MAP_ALLOC
		#include <stdlib.h>
		#include <crtdbg.h>
	#endif*/
	#define __MSXML_LIBRARY_DEFINED__
	#include "windows.h"
	#include <io.h>
	#ifdef CRT_SECURE
		#define access( exp1, exp2 ) _access_s( exp1, exp2 )
	#else
		#define access(x,y) _access( x, y )
	#endif
	#define sleep(x) Sleep( (x)*1000 )
#else
	#include <unistd.h>
#endif

#include <iostream>
#include <sstream>
//#include <fstream>

#include <vector>

#include <xalanc/XercesParserLiaison/XercesDocumentWrapper.hpp>

using namespace std;


#include "MQ/MqFilter.h"

#include "Trace.h"
#include "LogManager.h"

#include "PlatformDeps.h"
#include "XmlUtil.h"
#include "StringUtil.h"
#include "ConnectionString.h"
#include "Base64.h"

#include "AppSettings.h"
#include "BatchManager/Storages/BatchXMLfileStorage.h"

#include "DbPublisher.h"

#define MAX_BATCH_MSGS 10

//map< std::string, std::string, less< std::string > > DbPublisher::m_DadCache;

//constructor
DbPublisher::DbPublisher() : Endpoint(), m_Watcher( &m_NotificationPool ),
	m_WMQBatchFilter( false ), m_DatabaseProvider( "" ), m_DatabaseName( "" ), m_TableName( "" ), m_RepliesTableName( "" ), 
	m_UserName( "" ), m_UserPassword( "" ), m_DadOptions( DbDad::NODAD ), m_Dad( NULL ), m_SPinsertXmlData( "" ), m_CurrentMessageId( "" ), m_CurrentGroupId( "" ), 
	m_CurrentMessageLength( 0 ), m_CurrentDatabase( NULL ), m_CurrentProvider( NULL ), m_PrevBatchItem( 0 ), m_LastRequestor( "" ),
	m_BatchChanged( false ), m_TransactionStarted( false ), m_InsertBlob( false ), m_BlobLocator( "" ), m_BlobFilePattern( "" ), 
	m_CfgDatabaseName( "" ), m_CfgUserName( "" ), m_CfgUserPassword( "" ), m_DDSettings()
{
	DEBUG2( "CONSTRUCTOR" );
}
	
//destructor
DbPublisher::~DbPublisher()
{
	DEBUG2( "DESTRUCTOR" );

	try
	{
		if ( m_CurrentDatabase != NULL )
		{
			delete m_CurrentDatabase;
			m_CurrentDatabase = NULL;
		}
	}
	catch( ... )
	{
		try
		{
			TRACE( "An error occured while releasing current database" );
		} catch( ... ){}
	}

	try
	{
		if ( m_CurrentProvider != NULL )
		{
			delete m_CurrentProvider;
			m_CurrentProvider = NULL;
		}
	}
	catch( ... )
	{
		try
		{
			TRACE( "An error occured while releasing current database provider" );
		} catch( ... ){}
	}

	try
	{
		if ( m_Dad != NULL )
		{
			delete m_Dad;
			m_Dad = NULL;
		}
	}
	catch( ... )
	{
		try
		{
			TRACE( "An error occured while releasing dad instance" );
		} catch( ... ){}
	}
}

void DbPublisher::Init()
{

	DEBUG( "INIT" );
//TODO See if this is needed
//	INIT_COUNTERS( &m_DadCache, DadCache );
	INIT_COUNTERS( &m_Watcher, DBPublisherWatcher );

	// Read application settings 
	m_DatabaseProvider = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::DBPROVIDER );
	m_DatabaseName = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::DBNAME );
	m_UserName = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::DBUSER );
	m_UserPassword = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::DBPASS );

	// Read Blob handling options
	m_BlobLocator = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::BLOBLOCATOR, "" );
	m_BlobFilePattern = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::BLOBPATTERN, "" );	

	// Read config settings 
	m_CfgDatabaseName = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::DBCFGNAME, "" );
	m_CfgUserName = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::DBCFGUSER, "" );
	m_CfgUserPassword = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::DBCFGPASS, "" );

	// may be missing ( table will be chosen at runtime )
	m_TableName = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::TABLENAME, "" );

	// if replies table is not set, use the same table as upload table
	m_RepliesTableName = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::REPLIESTABLE, m_TableName );
	
	//Create Database Provider
	m_CurrentProvider = DatabaseProvider::GetFactory( m_DatabaseProvider );

	string dadFileName = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::DADFILE, "" );
	if( !dadFileName.empty() )
		m_Dad = new DbDad( dadFileName, m_CurrentProvider );
	m_AckDadFileName = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::ACKDADFILE, "" );

	m_SPinsertXmlData = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::SPINSERT, "" );
	string insertBlob = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::INSERTBLOB, "false" );
	m_InsertBlob = ( insertBlob == "true" );

	if( ( m_SPinsertXmlData.length() == 0 ) && ( dadFileName.length() == 0 ) )
	{
		throw runtime_error( "Neither SPinsertXmlData nor DadFileName options specified in config, use one to send message to App Database" );
	}

	string dadOptions = StringUtil::ToUpper( getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::DADOPTIONS, "NODAD" ) );
	if ( dadOptions == "NODAD" )
		m_DadOptions = DbDad::NODAD;

	if ( m_CurrentProvider->name() == "Informix" )
		m_DadOptions = DbDad::WITHVALUES;

	if ( dadOptions == "WITHPARAMS" )
		m_DadOptions = DbDad::WITHPARAMS;

	//Create Database
	m_CurrentDatabase = m_CurrentProvider->createDatabase();

	// get duplicate detection settings
	if ( m_CfgDatabaseName.length() > 0 )
	{
		DEBUG( "Reading duplicate detection settings... " );
		m_DDSettings.GetDuplicateServices( m_CurrentProvider, m_CfgDatabaseName, m_CfgUserName, m_CfgUserPassword );
	}
	else
	{
		DEBUG( "Not using duplicate detection. [No cfg database name/user/pass specified]" );
	}

	// expect the first filter to be of WMQ type
	AbstractFilter* firstFilter = ( *m_FilterChain )[ 0 ];
	if ( ( firstFilter == NULL ) || ( firstFilter->getFilterType() != FilterType::MQ ) )
		throw logic_error( "First filter in a publisher's chain should be of MQ type. Please check the config file." );

	MqFilter* getterFilter = dynamic_cast< MqFilter* >( firstFilter );
	if ( getterFilter == NULL )
		throw logic_error( "First filter in a publisher's chain should be of MQ type. Please check the config file." );

	m_WMQBatchFilter = getterFilter->isBatch();
	
	// only add one recor/batch, only add unique records to pool
	if( m_WMQBatchFilter )
		m_Watcher.setWatchOptions( MqWatcher::NotifyGroups | MqWatcher::NotifyUnique );

	m_WatchQueue = getterFilter->getQueueName();
	m_WatchQueueManager = getterFilter->getQueueManagerName();
	m_WatchTransportURI = getterFilter->getTransportURI();
	m_Watcher.setHelperType( getterFilter->getHelperType() );

	DEBUG( "Watch queue [" << m_WatchQueue << "] on queue manager [" << m_WatchQueueManager << "] using channel definition [" << m_WatchTransportURI << "]" );

	m_Watcher.setQueue( m_WatchQueue );
	m_Watcher.setQueueManager( m_WatchQueueManager );
	m_Watcher.setTransportURI( m_WatchTransportURI );

	m_PrevBatchItem = -1;

	if ( haveGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::BATCHMGRTYPE ) )
	{
		string batchManagerType = getGlobalSetting( EndpointConfig::WMQToApp, EndpointConfig::BATCHMGRTYPE );		
		try
		{
			if ( batchManagerType == "XMLfile" )
			{
				m_BatchManager = BatchManagerBase::CreateBatchManager( BatchManagerBase::XMLfile );
			}
			else
			{
				TRACE( "Batch manager type [" << batchManagerType << "] not implemented. No batch processing will be performed." );
			}
		}
		catch( const std::exception& ex )
		{
			TRACE( "Batch manager could not be created. [" << ex.what() << "]" );
			if ( m_BatchManager != NULL )
			{
				delete m_BatchManager;
				m_BatchManager = NULL;
			}
			throw;
		}
		catch( ... )
		{
			TRACE( "Batch manager could not be created. [unknown exception]" );
			if ( m_BatchManager != NULL )
			{
				delete m_BatchManager;
				m_BatchManager = NULL;
			}
			throw;
		}
	}
	else
	{
		DEBUG( "No batch manager specified in config file." );
	}
}

void DbPublisher::internalStart()
{
	DEBUG( "Starting watcher... " );
	m_Watcher.setEnableRaisingEvents( true );

	TRACE_SERVICE( "[" << m_ServiceThreadId << "] starting to process... " );

	try
	{
		while( m_Running )
		{
			// test sigsegv
			// pthread_kill( m_SelfThreadId, SIGSEGV );
			
			m_TransactionStarted = false;

			DEBUG( "Publisher [" << m_SelfThreadId << "] waiting for notifications in pool" );
			WorkItem< AbstractWatcher::NotificationObject > notification = m_NotificationPool.removePoolItem();
			
			AbstractWatcher::NotificationObject *notificationObject = notification.get();
			
			m_CurrentMessageId = notificationObject->getObjectId();
			m_CurrentMessageLength = notificationObject->getObjectSize();

			DEBUG( "Notified : [" << m_CurrentMessageId << "] in group [" << notificationObject->getObjectGroupId()
				<< "] size [" << m_CurrentMessageLength << "]" );
	
			//TODO throw only on fatal error. The connector should respawn this thread
			bool succeeded = true;
			m_IsLast = false;

			try
			{
				DEBUG( "Performing message loop ... " );

				// we don't need to the the endpoint that the message may be a batch...
				// we either upload to DB2 or to the batch manager
				succeeded = PerformMessageLoop( false );
				DEBUG( "Message loop finished ok. " );
			}
			catch( const AppException& ex )
			{
				string exceptionType = typeid( ex ).name();
				string errorMessage = ex.getMessage();
				
				TRACE_GLOBAL( exceptionType << " encountered while processing message : " << errorMessage );
				succeeded = false;
			}
			catch( const std::exception& ex )
			{
				string exceptionType = typeid( ex ).name();
				string errorMessage = ex.what();
				
				TRACE_GLOBAL( exceptionType << " encountered while processing message : " << errorMessage );
				succeeded = false;
			}
			catch( ... )
			{
				TRACE_GLOBAL( "[unknown exception] encountered while processing message. " );
				succeeded = false;
			}

			if ( !succeeded && m_Running )
			{
				TRACE( "Sleeping 10 seconds before next attempt( previous message failed )" );
				sleep( 10 );
			}
		} //while true
	}
	catch( const WorkPoolShutdown& shutdownError )
	{
		TRACE_GLOBAL( shutdownError.what() );
	}
	catch( const std::exception& ex )
	{
		string exceptionType = typeid( ex ).name();
		TRACE_GLOBAL( exceptionType << " encountered while processing message : " << ex.what() );
	}
	catch( ... )
	{
		TRACE_GLOBAL( "Unhandled exception encountered while processing. " );
	}
}

void DbPublisher::internalStop()
{
	DEBUG( "STOP" );
	
	// ensure watcher is dead ( the watcher will lock on pool and wait until it is empty )
	m_Watcher.setEnableRaisingEvents( false );

	// the endpoint must now be locked on pool
	// the watcher must be dead by now...
	m_Running = false;

	m_NotificationPool.ShutdownPool();
	
	TRACE( m_ServiceThreadId << " joining endpoint ..." );
	int joinResult = pthread_join( m_SelfThreadId, NULL );
	if ( 0 != joinResult )
	{
		TRACE( "Joining self thread ended in error [" << joinResult << "]" );
	}

	//Disconnect from database
	if ( m_CurrentDatabase != NULL )
		m_CurrentDatabase->Disconnect();
}

string DbPublisher::Prepare()
{
	DEBUG2( "PREPARE" );
	
	//sleep( 5 ); //TODO remove this when everything works ok
	
	//m_FilterChain->Report( true, true );
	if ( m_CurrentDatabase == NULL )
	{
		TRACE( "Prepare failed : Database not configured" );
		throw runtime_error( "Prepare failed : Database not configured" );
	}

	//TODO check connection to currentDatabase 
	if ( !m_CurrentDatabase->IsConnected() )
	{
		DEBUG( "Connecting to database ... " );
		
		unsigned int connectCount = 0;
		bool connected = false;

		do
		{
			try
			{
				m_CurrentDatabase->Connect( ConnectionString( m_DatabaseName, m_UserName, m_UserPassword ) );
				connected = m_CurrentDatabase->IsConnected();
			}
			catch( const std::exception& ex )
			{
				// unable to connect ...
				if ( connectCount == 0 )
				{
					AppException aex( "Unable to connect to database ... retrying", ex );
					aex.addAdditionalInfo( "Database name", m_DatabaseName );
					aex.addAdditionalInfo( "User name", m_UserName );

					LogManager::Publish( aex );
				}
			}
			catch( ... )
			{
				// unable to connect ...
				if ( connectCount == 0 )
				{
					AppException aex( "Unable to connect to database ... retrying" );
					aex.addAdditionalInfo( "Database name", m_DatabaseName );
					aex.addAdditionalInfo( "User name", m_UserName );

					LogManager::Publish( aex );
				}
			}

			connectCount++;
			if( !connected && m_Running )
			{
				TRACE( "Sleeping 30 seconds before next attempt( previous message failed )" );
				sleep( 30 );
			}

		} while ( !connected && m_Running );
		if ( !connected )
			throw runtime_error( "Unable to connect to database" );
	}
	else
	{
		DEBUG( "Already connected to database ... " );
	}
	
	if ( m_BlobFilePattern.length() > 0 )
	{
		StringUtil blobFilePattern( m_BlobFilePattern );
		blobFilePattern.Split( "{" );
		string blobsPath = blobFilePattern.NextToken();
		if ( ( access( blobsPath.c_str(), 2 ) ) == -1 )
		{	// you don't have rights to access this folder
			stringstream errorMessage;
			errorMessage << "Don't have write permission to the source folder[" << blobsPath << "].";
			TRACE( errorMessage.str() );

			throw AppException( errorMessage.str() );		
		}
	}
	
	return m_CurrentMessageId;
}

void DbPublisher::Process( const string& correlationId )
{
	//This will change after batch support is in place
	DEBUG2( "PROCESS" );	
	
	//Get data from MQ
	XERCES_CPP_NAMESPACE_QUALIFIER DOMDocument *doc = NULL;
	// create the document 
	DOMImplementation* impl = DOMImplementationRegistry::getDOMImplementation( unicodeForm( "LS" ) );
	doc = impl->createDocument( 0, unicodeForm( "TableName" ), 0 );
	
	string xmlTable = m_TableName;
	string theDadFileName = m_Dad?m_Dad->tableName():"";

	try
	{
		m_TransportHeaders.Clear();
		
		m_TransportHeaders.Add( MqFilter::MQMSGSIZE, StringUtil::ToString( m_CurrentMessageLength ) );		
		m_TransportHeaders.Add( MqFilter::MQMSGID, m_CurrentMessageId );	

		( void )m_FilterChain->ProcessMessage( doc, m_TransportHeaders, false );

		if ( m_TransportHeaders.ContainsKey( MqFilter::MQMESSAGETYPE ) && ( m_TransportHeaders[ MqFilter::MQMESSAGETYPE ] == "REPLY" ) )
		{
            theDadFileName = m_AckDadFileName;
			xmlTable = m_RepliesTableName;

            DEBUG( "Using DAD [" << theDadFileName << "] to upload reply" );
        }

	}
	catch( ... ) //TODO put specific catches before this
	{	
		if( doc != NULL )
		{
			doc->release();
			doc = NULL;
		}
		//TODO add details to the error 
		throw;
	}
	
	DEBUG( "Message processed in the chain." );
	if( doc == NULL )
	{
		TRACE( "Document received from filter chain is empty." );
		throw runtime_error( "Document received from filter chain is empty." );
	}

	//string theTableName = xmlTable;
	//Serialize DOMDocument
	//string xmlData = "";
	string theRequestor, theResponder, theCorrelationId;
	string innerXmlData = "";
	string ddHash = "";
	string ddInput = "";

	try
	{
		XALAN_USING_XALAN( XercesDocumentWrapper );
		XALAN_USING_XALAN( XalanDocument );

		// map xerces dom to xalan document
#ifdef XALAN_1_9
		XALAN_USING_XERCES( XMLPlatformUtils )
		XercesDocumentWrapper docWrapper( *XMLPlatformUtils::fgMemoryManager, doc, true, true, true );
#else
		XercesDocumentWrapper docWrapper( doc, true, true, true );
#endif
		XalanDocument* theDocument = ( XalanDocument* )&docWrapper;	
		
		{ // Xalan required block
			theRequestor = XPathHelper::SerializeToString( XPathHelper::Evaluate( "/qPCMessageSchema/Message/RequestorService/child::text()", theDocument ) );
			theResponder = XPathHelper::SerializeToString( XPathHelper::Evaluate( "/qPCMessageSchema/Message/ResponderService/child::text()", theDocument ) );
			theCorrelationId = XPathHelper::SerializeToString( XPathHelper::Evaluate( "/qPCMessageSchema/Message/CorrelationId/child::text()", theDocument ) );
			
			if ( m_DDSettings.IsDDActive( theRequestor ) )
			{
				ddInput = Base64::decode( XPathHelper::SerializeToString( XPathHelper::Evaluate( "/qPCMessageSchema/Message/Payload/Transformed/child::text()", theDocument ) ) );
			}

			if ( theCorrelationId.length() != 0 )
				setCorrelationId( theCorrelationId );
	
			//theDadFileName este completat doar pe conector spre BackOffice
			//trateaza situatia cand vine de la qPay spre BackOffice
			//datele sunt in base 64 si fac decode
			if( m_Dad != NULL )
			{
				string msgData = XmlUtil::SerializeToString( doc );
				// hack .. the sp m_InsertBlob is specified for messages "toqpay" only
				if ( !m_InsertBlob )
					m_XmlData = Base64::decode( XPathHelper::SerializeToString( XPathHelper::Evaluate( "/qPCMessageSchema/Message/Payload/child::text()", theDocument ) ) );
				else
				{
					// hack to remove blob data
					innerXmlData = Base64::decode( XPathHelper::SerializeToString( XPathHelper::Evaluate( "/qPCMessageSchema/Message/Payload/Original/child::text()", theDocument ) ) );

					if ( innerXmlData.length() > 0 )
					{
						const DOMElement* root = doc->getDocumentElement();
						DOMNodeList* msgNodes = root->getElementsByTagName( unicodeForm( "Message" ) );
						if ( ( msgNodes == NULL ) || ( msgNodes->getLength() == 0 ) )
							throw runtime_error( "Missing required [Message] element in [qPCMessageSchema] document" );

						const DOMElement* msgNode = dynamic_cast< DOMElement* >( msgNodes->item( 0 ) );
						if ( msgNode == NULL )
							throw logic_error( "Bad type : [qPCMessageSchema/Message[0]] should be and element" );

						// get an attribute that overrides dad options
						XERCES_CPP_NAMESPACE_QUALIFIER DOMAttr* useDad = msgNode->getAttributeNode( unicodeForm( "useDAD" ) );
						if ( useDad == NULL )
							theDadFileName = "";

						DOMNodeList* payNodes = msgNode->getElementsByTagName( unicodeForm( "Payload" ) );
						if ( ( payNodes == NULL ) || ( payNodes->getLength() == 0 ) )
							throw runtime_error( "Missing required [Payload] element in [qPCMessageSchema/Message[0]] document" );

						DOMElement* payNode = dynamic_cast< DOMElement* >( payNodes->item( 0 ) );
						if ( payNode == NULL )
							throw logic_error( "Bad type : [qPCMessageSchema/Message[0]/Payload[0]] should be and element" );

						DOMNodeList* orgNodes = payNode->getElementsByTagName( unicodeForm( "Original" ) );
						if ( ( orgNodes == NULL ) || ( orgNodes->getLength() == 0 ) )
							throw runtime_error( "Missing required [Original] element in [qPCMessageSchema/Message[0]/Payload[0]] document" );

						DOMElement* orgNode = dynamic_cast< DOMElement* >( orgNodes->item( 0 ) );
						if ( orgNode == NULL )
						{
							// try to get the payload without looking for <Original> ( for BO connectors )
							if ( payNode->getFirstChild() != NULL )
							{
								DOMNode* theRemovedChild = payNode->removeChild( payNode->getFirstChild() );
	 							theRemovedChild->release();
								payNode->setTextContent( unicodeForm( "Payload removed" ) );
							}
							else
								throw logic_error( "Could not find payload in [qPCMessageSchema/qPCMessageSchema/Message[0]/Payload[0]]" );
						}
						else 
						{
							if ( orgNode->getFirstChild() != NULL )
							{
								DOMNode* theRemovedChild = orgNode->removeChild( orgNode->getFirstChild() );
	 							theRemovedChild->release();
								orgNode->setTextContent( unicodeForm( "Payload removed" ) );
							}
							else
								throw logic_error( "Could not find payload in [qPCMessageSchema/qPCMessageSchema/Message[0]/Payload[0]]" );
						}
					}

					m_XmlData = XmlUtil::SerializeToString( doc );
				}
			}
			else
			// theDadFileName nu este completat  pe conector spre qPay server
			// trateaza situatia cand vine de la  BackOffice/RTGS/ACH spre qPay
			// datele sunt in deja in base 64 si nu fac nimic
			{
				if ( m_BatchManager != NULL )
				{
					// in batch mode, we only need to keep the <Message></Message> part ( the envelope will be appended by BatchManager )
					m_XmlData = XmlUtil::SerializeNodeToString( doc->getFirstChild()->getFirstChild() );
				}
				else
				{
					m_XmlData = XmlUtil::SerializeToString( doc );
				}
				//theDadFileName.assign( theRequestor.append( "Queue.dad" ) );
				theDadFileName = theRequestor + string( "Queue.dad" );
			}

			//pentru Oracle (la DB2 nu are semnficatie acest parametru)
			// m_TableName nu se va completa in situatia cand vreau sa introduc in qPay
			// unde va trebui sa stabilesc tabela in care introduc in functie de requestor		
			if ( m_TableName.length() == 0 )
			{
				xmlTable = theRequestor + string( "Queue" );
				DEBUG( "qPay table name : " << xmlTable );
			}
			// pentru cazul in care introduc in BackOffice, se va preciza in fisierul de configurare
			// numele tabelei in care se vor introduce datele
			if ( ( m_BlobLocator.length() > 0 ) && ( m_BlobFilePattern.length() > 0 ) )
				m_XmlData = SaveBlobToFile( m_XmlData );

			m_XmlData = StringUtil::Replace( m_XmlData, "'", "&apos;" );
		}
	}
	catch( const std::exception& error )
	{
		stringstream messageBuffer;
		messageBuffer << typeid( error ).name() << " Exception [" << error.what() << "]";
		
		TRACE( messageBuffer.str() );
		
		if( doc != NULL )
		{
			doc->release();
			doc = NULL;
		}  			
		throw;
	}
	catch( ... )
	{
		TRACE( "Unhandled exception !" );
		
		if( doc != NULL )
		{
			doc->release();
			doc = NULL;
		}  			
		throw;
	}
	
	if( doc != NULL )
	{
		doc->release();
		doc = NULL;
	}
	
	m_IsLast = true;
	m_BatchChanged = false;

	if ( theRequestor.length() == 0 )
	{
		AppException aex( "Requestor not found in the received message" );
		aex.setSeverity( EventSeverity::Fatal );
		throw aex;
	}

	if ( m_BatchManager != NULL )
	{		
		m_PrevBatchItem++;
		m_BatchChanged = false;

		// if the DAD changed
		if ( ( m_PrevBatchItem != 0 ) && ( m_LastRequestor != theDadFileName ) )
		{
			string messageOutput = "";

			DEBUG( "Requestor changed, closing previous batch." );
						
			//serialize XML
			if ( m_BatchManager->getStorageCategory() == BatchManagerBase::XMLfile )
			{
				BatchManager< BatchXMLfileStorage >* batchManager = dynamic_cast< BatchManager< BatchXMLfileStorage >* >( m_BatchManager );
				if ( batchManager == NULL )
					throw logic_error( "Bad type : batch manager is not of XML type" );

				// get batch xml document
				messageOutput = batchManager->storage().getSerializedXml();

				if ( m_DDSettings.IsDDActive( theRequestor ) )
				{
					ddHash = m_DDSettings.Hash( theRequestor, messageOutput );
				}
			}

			m_BatchManager->close( m_CurrentGroupId );
			m_PrevBatchItem = 0;

			// we need to upload the previous batch and open a new one
			UploadMessage( theDadFileName, messageOutput, xmlTable, ddHash );
			m_BatchChanged = true;
		}

		m_IsLast = false;

		// if this is the first item
		if ( m_PrevBatchItem == 0 )
		{
			DEBUG_GLOBAL( "First batch item ... opening storage" );
		
			m_LastRequestor = theDadFileName;

			// generate a unique groupid every time
			m_CurrentGroupId = Collaboration::GenerateGuid();

			// open batch ( next iteration it will already be open )
			m_BatchManager->open( m_CurrentGroupId, ios_base::out );
			
			// insert envelope elem
			*m_BatchManager << "<qPCMessageSchema/>";
		}
		
		BatchItem batchItem;
		batchItem.setPayload( m_XmlData );
		batchItem.setBatchId( m_CurrentGroupId );
		batchItem.setMessageId( m_CurrentMessageId );

		DEBUG( "Add message to batch" );
		*m_BatchManager << batchItem;

		// the maximum uncommited messages was reached or there are no more messages to process, close the batch
		if( ( m_PrevBatchItem > MAX_BATCH_MSGS ) || ( m_NotificationPool.getSize() == 0 ) )
		{
			string messageOutput = "";

			DEBUG( "LAST message [" << m_PrevBatchItem << "]" );
						
			//serialize XML
			if ( m_BatchManager->getStorageCategory() == BatchManagerBase::XMLfile )
			{
				BatchManager< BatchXMLfileStorage >* batchManager = dynamic_cast< BatchManager< BatchXMLfileStorage >* >( m_BatchManager );
				if ( batchManager == NULL )
					throw logic_error( "Bad type : batch manager is not of XML type" );

				// get batch xml document
				messageOutput = batchManager->storage().getSerializedXml();

				if ( m_DDSettings.IsDDActive( theRequestor ) )
				{
					ddHash = m_DDSettings.Hash( theRequestor, messageOutput );
				}
			}

			m_BatchManager->close( m_CurrentGroupId );
			m_IsLast = true;
			m_PrevBatchItem = -1;

			UploadMessage( theDadFileName, messageOutput, xmlTable, ddHash );
		}
	}
	else
	{
		if ( m_DDSettings.IsDDActive( theRequestor ) )
		{
			ddHash = m_DDSettings.Hash( theRequestor, ddInput );
		}
		UploadMessage( theDadFileName, ( innerXmlData.length() > 0 ) ? innerXmlData : m_XmlData, xmlTable, ddHash );
	}	
}

void DbPublisher::UploadMessage( const string& theDadFileName, const string& xmlData, const string& xmlTable, const string& hash )
{
	if ( m_CurrentProvider == NULL )
		throw runtime_error( "Database provider not initialized" );

	if ( m_CurrentDatabase == NULL )
		throw runtime_error( "Database not initialized" );
#ifndef NO_DAD
	// flags in the message may override dad settings
	if ( ( m_DadOptions != DbDad::NODAD ) && ( theDadFileName.length() > 0 ) )
	{
		// Begin transaction
		m_CurrentDatabase->BeginTransaction();
		m_TransactionStarted = true;

		m_Dad->Upload( xmlData, m_CurrentDatabase, ( m_DadOptions == DbDad::WITHPARAMS ) );

		// if the insertxml sp name is present, continue and upload the message, but with empty xmlData
		if ( m_SPinsertXmlData.length() == 0 )
			return;
	}
#endif //NO_DAD

	//Insert data into DB using a stored procedure
	ParametersVector myParams;
	DataParameterBase *paramDadFile = m_CurrentProvider->createParameter( DataType::CHAR_TYPE );
	DataParameterBase *paramXmlBuffer = m_CurrentProvider->createParameter( DataType::LARGE_CHAR_TYPE );
	DataParameterBase *paramXmlTable = m_CurrentProvider->createParameter( DataType::CHAR_TYPE );
	DataParameterBase *paramLengthBuffer = m_CurrentProvider->createParameter( DataType::CHAR_TYPE );
	DataParameterBase *paramReturnCode = m_CurrentProvider->createParameter( DataType::CHAR_TYPE, DataParameterBase::PARAM_OUT );
	DataParameterBase *paramReturnMessage = m_CurrentProvider->createParameter( DataType::CHAR_TYPE, DataParameterBase::PARAM_OUT );
		
	try
	{
		// 1st param : DAD buffer used by DB2 only
		string dadBuffer = "";
		if ( m_CurrentProvider->name() == "DB2" )
			dadBuffer = getDad( theDadFileName );
		else
			dadBuffer = hash;

		DEBUG2( "DAD contents : " << dadBuffer );
		paramDadFile->setDimension( dadBuffer.length() );	  	 
		paramDadFile->setString( dadBuffer );
		paramDadFile->setName( "DAD_content" );
		myParams.push_back( paramDadFile );

		// 2nd param : XML message from MQ
		DEBUG2( "XML buffer: " << m_XmlData );
		/*if ( m_DadOptions != DbDad::NODAD )
		{
			paramXmlBuffer->setDimension( 0 );
			paramXmlBuffer->setString( "" );
		}
		else*/
		{
			paramXmlBuffer->setDimension( m_XmlData.length() );
			paramXmlBuffer->setString( m_XmlData );
		}
		paramXmlBuffer->setName( "XML_message" );
		myParams.push_back( paramXmlBuffer );

		// 3rd param : Table name used by Oracle only
		paramXmlTable->setDimension( xmlTable.length() );		
		paramXmlTable->setString( xmlTable );
		paramXmlTable->setName( "table_name" );
		myParams.push_back( paramXmlTable );
		
		// 4th param : Buffer size
		string bufferSize = StringUtil::ToString( m_XmlData.length() );
		paramLengthBuffer->setDimension( bufferSize.length() );
		paramLengthBuffer->setString( bufferSize );
		paramLengthBuffer->setName( "buffer_size" );
		myParams.push_back( paramLengthBuffer );

		// 5th param : return code from upload proc
		paramReturnCode->setDimension( 11 );
		myParams.push_back( paramReturnCode );
		
		// 6th param : return message from upload proc
		paramReturnMessage->setDimension( 1024 );
		myParams.push_back( paramReturnMessage );

		// Begin transaction
		if ( !m_TransactionStarted )
			m_CurrentDatabase->BeginTransaction();
		m_TransactionStarted = true;

		// Execute the Stored Procedure that will call dxxShredXML procedure 
		m_CurrentDatabase->ExecuteNonQueryCached( DataCommand::SP, m_SPinsertXmlData, myParams ); 	
	   
		string returnCode = StringUtil::Trim( paramReturnCode->getString() );

		DEBUG( "Insert XML data return code : [" << returnCode << "]" );
		
		//sqlserver nonquery hack ( no return )
		if ( ( returnCode.length() > 0 ) && ( returnCode != "0" ) )
	 	{
	 		stringstream messageBuffer;
	  		messageBuffer << "Error inserting XML. Message : [" << paramReturnMessage->getString() << "]";
			TRACE( messageBuffer.str() );
			myParams.Dump();

	 		throw runtime_error( messageBuffer.str() );
	 	}	  
	}
	catch( const DBConnectionLostException& error )
	{
		stringstream messageBuffer;
	  	messageBuffer << typeid( error ).name() << " exception [" << error.what() << "]";
		TRACE( messageBuffer.str() );
		TRACE( "Original buffer [" << xmlData << "]" );
		throw;		
	}
	catch( const std::exception& error )
	{
		stringstream messageBuffer;
	  	messageBuffer << typeid( error ).name() << " exception [" << error.what() << "]";
		TRACE( messageBuffer.str() );
		TRACE( "Original buffer [" << xmlData << "]" );
		throw;
	}
	catch( ... )
	{
		TRACE( "An error occured while uploading the message to the database [unknown error]" );
		TRACE( "Original buffer [" << xmlData << "]" );
		throw;
	}
}

void DbPublisher::Commit()
{
	DEBUG2( "COMMIT" );
	
	if( m_IsLast || m_BatchChanged )
	{
		if ( m_CurrentDatabase == NULL )
			throw runtime_error( "Database not initialized" );

		m_CurrentDatabase->EndTransaction( TransactionType::COMMIT );
		m_TransactionStarted = false;
		m_FilterChain->Commit();
	}
	else
	{
		DEBUG( "Partial commit for batch item" );
	}
}

void DbPublisher::Abort()
{
	DEBUG( "ABORT" );
	
	m_FilterChain->Abort();

	//rollback db insert
	try
	{
		if ( m_CurrentDatabase == NULL )
			throw runtime_error( "Database not initialized" );
		m_CurrentDatabase->EndTransaction( TransactionType::ROLLBACK );
	}
	catch( ... )
	{
		// supress error messages ( Rollback transaction failed : OCI_INVALID_HANDLE )
		if ( m_TransactionStarted )
		{
			m_TransactionStarted = false;
			throw;
		}
	}
	m_TransactionStarted = false;
}

void DbPublisher::Rollback()
{
	DEBUG2( "ROLLBACK" );
	
	m_FilterChain->Rollback();

	//rollback db insert
	try
	{
		if ( m_CurrentDatabase == NULL )
			throw runtime_error( "Database not initialized" );
		m_CurrentDatabase->EndTransaction( TransactionType::ROLLBACK );
	}
	catch( ... )
	{
		// supress error messages ( Rollback transaction failed : OCI_INVALID_HANDLE )
		if ( m_TransactionStarted )
		{
			m_TransactionStarted = false;
			throw;
		}
	}
	m_TransactionStarted = false;
}

string DbPublisher::getDad( const string& dadFilename )
{
	// First parameter (Input): DAD buffer
	if ( m_DadCache.Contains( dadFilename ) )
		return m_DadCache[ dadFilename ];
	else
	{
		DEBUG_GLOBAL( "Adding requested DAD [" << dadFilename << "] to cache..." );
		string dadBuffer = StringUtil::DeserializeFromFile( dadFilename );
		
		m_DadCache.Add( dadFilename, dadBuffer );
		DEBUG_GLOBAL( "Returning DAD from cache ..." );
		return m_DadCache[ dadFilename ];
	}
}

string DbPublisher::SaveBlobToFile( const string& xmlData )
{
	string result = xmlData;

	XERCES_CPP_NAMESPACE_QUALIFIER DOMDocument *doc = NULL;
	//stream nedeed to put image to disk
	ofstream outf;
	string destinationBlobFilename;
	try
	{
		doc = XmlUtil::DeserializeFromString( xmlData );
		if ( doc == NULL ) 
			return result;

		XALAN_USING_XALAN( XercesDocumentWrapper );
		XALAN_USING_XALAN( XalanDocument );

		// map xerces dom to xalan document
#ifdef XALAN_1_9
		XALAN_USING_XERCES( XMLPlatformUtils )
		XercesDocumentWrapper docWrapper( *XMLPlatformUtils::fgMemoryManager, doc, true, true, true );
#else
		XercesDocumentWrapper docWrapper( doc, true, true, true );
#endif
		XalanDocument* theDocument = ( XalanDocument* )&docWrapper;	

		{ // Xalan required block
			string theBlob = XPathHelper::SerializeToString( XPathHelper::Evaluate( m_BlobLocator + "/child::text()", theDocument ) );
			if ( theBlob.length() <= 0 )
			{
				if( doc != NULL )
				{
					doc->release();
					doc = NULL;
				}  
				return result;
			}

			//parse the blob locator xpath and remove the blob from doc
			vector< string > locatorBits;
			StringUtil pathSplitter( m_BlobLocator );
			pathSplitter.Split( "/" );
			while( pathSplitter.MoreTokens() )
			{
				string crtParam = StringUtil::Trim( pathSplitter.NextToken() );
				if ( crtParam.length() > 0 )
					locatorBits.push_back( crtParam );
			};

			//iterate through the locator bits and find the content
			string pathSoFar = "";
			DOMElement* root = doc->getDocumentElement();

			// skip root node
			vector< string >::const_iterator bitsWalker = locatorBits.begin();
			bitsWalker++;

			for( ; bitsWalker != locatorBits.end(); bitsWalker++ )
			{
				DOMNodeList* nextNodes = root->getElementsByTagName( unicodeForm( *bitsWalker ) );
				if ( ( nextNodes == NULL ) || ( nextNodes->getLength() == 0 ) )
					throw runtime_error( "Missing required [] element in [] document" );

				root = dynamic_cast< DOMElement* >( nextNodes->item( 0 ) );
				if ( root == NULL )
					throw logic_error( "Bad type : [/[0]] should be and element" );
			}

			// remove blob node
			if ( root->getFirstChild() != NULL )
			{
				DOMNode* theRemovedChild = root->removeChild( root->getFirstChild() );
				theRemovedChild->release();
				root->setTextContent( unicodeForm( "[removed]" ) );
			}
			else
				throw logic_error( "Could not find payload in []" );

			//find tokens in blobfilepattern
			vector< string > patternTokens;
			string::size_type lastSeparator = 0;

			string::size_type nextOpenSeparator = m_BlobFilePattern.find_first_of( '{', lastSeparator );
			string::size_type nextCloseSeparator = m_BlobFilePattern.find_first_of( '}', lastSeparator );

			while ( ( nextOpenSeparator != string::npos ) && ( nextCloseSeparator != string::npos ) )
			{
				patternTokens.push_back( m_BlobFilePattern.substr( nextOpenSeparator + 1, nextCloseSeparator - nextOpenSeparator - 1 ) );
				lastSeparator = nextCloseSeparator + 1;

				nextOpenSeparator = m_BlobFilePattern.find_first_of( '{', lastSeparator );
				nextCloseSeparator = m_BlobFilePattern.find_first_of( '}', lastSeparator );
			}

			//find tokens in message and replace in path
			destinationBlobFilename = m_BlobFilePattern;
			for( vector< string >::const_iterator tokensWalker = patternTokens.begin(); tokensWalker != patternTokens.end(); tokensWalker++ )
			{
				string theField = XPathHelper::SerializeToString( XPathHelper::Evaluate( ( *tokensWalker ) + "/child::text()", theDocument ) );
				destinationBlobFilename = StringUtil::Replace( destinationBlobFilename, "{" + *tokensWalker + "}", theField );
			}

			//StringUtil::SerializeToFile( destinationBlobFilename, theBlob );
			string decodedBlob = Base64::decode( theBlob );
			
			outf.exceptions( ifstream::failbit | ifstream::badbit );
			outf.open( destinationBlobFilename.c_str(), ios_base::out | ios_base::binary );
			outf.write( decodedBlob.c_str(), decodedBlob.size() );
			outf.close();
			
			result = XmlUtil::SerializeToString( doc );
		}
	}
	catch( ofstream::failure& ex )
	{
		if( doc != NULL )
		{
			doc->release();
			doc = NULL;
		}
		stringstream errorMessage;
		errorMessage << "Error writing image [" << destinationBlobFilename << "] to repository";
		TRACE( errorMessage.str() << "Error is:[" << ex.what() << "]" );
		try
		{
			outf.close();
		}catch( ... ){};
		
		throw AppException( errorMessage.str(), ex );		
	}
	catch( ... )
	{
		if( doc != NULL )
		{
			doc->release();
			doc = NULL;
		}
		throw;
	}
	if( doc != NULL )
	{
		doc->release();
		doc = NULL;
	}  
	return result;
}

const string ConnectorsDDInfo::Hash( const string& connectorName, const string& payload )
{
	string transformFilename = m_Services[ connectorName ];

	DEBUG( "Generating message hash for [" << connectorName << "] using xslt [" << transformFilename << "] ..." );

	string hash = "";
	if ( transformFilename.length() > 0 )
	{
		XERCES_CPP_NAMESPACE_QUALIFIER DOMDocument* document = NULL;
		unsigned char **buffer = new ( unsigned char * ); 
		char *outputXslt = NULL;
			
		try
		{
			XSLTFilter finalTransform;
			NameValueCollection trpHeaders;
			
			trpHeaders.Add( XSLTFilter::XSLTUSEEXT, "true" );
			trpHeaders.Add( XSLTFilter::XSLTFILE, transformFilename );
			
			document = XmlUtil::DeserializeFromString( payload );
	  		finalTransform.ProcessMessage( document, buffer, trpHeaders, true );
			outputXslt = ( char* )( *buffer );
			hash = outputXslt;
			
			// release buffer
			if( document != NULL )
			{
				document->release();
				document = NULL;
			}
			if( outputXslt != NULL )
			{
				delete[] outputXslt;
				outputXslt = NULL;
			}
				
			if( buffer != NULL )
			{
				delete buffer;
				buffer = NULL;
			}
		}
		catch( ... )
		{		
			TRACE( "An error occured while generating message hash. Check [" << connectorName << "] exitpoint definition and file availability for [" << transformFilename << "]" );
			// release buffer
			if( document != NULL )
			{
				document->release();
				document = NULL;
			}
			if( outputXslt != NULL )
			{
				delete[] outputXslt;
				outputXslt = NULL;
			}
				
			if( buffer != NULL )
			{
				delete buffer;
				buffer = NULL;
			}
			throw;
		}
	}
	return hash;
}

void ConnectorsDDInfo::GetDuplicateServices( DatabaseProviderFactory *databaseProvider, const string& databaseName, const string& user, const string& password )
{
	m_Active = true;
	m_Services.clear();

	Database* config = NULL;

	DEBUG_GLOBAL( "Getting services that have a duplicate check set..." )
	DataSet* result = NULL;

	try
	{
		config = databaseProvider->createDatabase();
		config->Connect( ConnectionString( databaseName, user, password ) );

		config->BeginTransaction( true );
		result = config->ExecuteQuery( DataCommand::SP, "GetDuplicateSettings" );

		for ( unsigned int i=0; i<result->size(); i++ )
		{
			if ( result->getCellValue( i, "DUPLICATESERVICE" )->getInt() == 1 )
			{
				m_Services.insert( pair< string, string >( 
					StringUtil::Trim( result->getCellValue( i, "FRIENDLYNAME" )->getString() ),
					StringUtil::Trim( result->getCellValue( i, "DUPLICATEMAP" )->getString() ) ) );
			}
		}
		config->EndTransaction( TransactionType::COMMIT );
	}
	catch( const std::exception& ex )
	{
		if ( config != NULL )
		{
			config->EndTransaction( TransactionType::ROLLBACK );
			delete config;
		}
		TRACE( "There was an error obtaining the duplicate settings for connectors [" << ex.what() << ". No duplicate checks will be performed." );
	}
	catch( ... )
	{
		if ( config != NULL )
		{
			config->EndTransaction( TransactionType::ROLLBACK );
			delete config;
		}
		TRACE( "There was an error obtaining the duplicate settings for connectors. No duplicate checks will be performed." );
	}
	if ( config != NULL )
	{
		delete config;
		config = NULL;
	}
	if ( result != NULL )
	{
		delete result;
		result = NULL;
	}
}
