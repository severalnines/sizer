/* Copyright (C) 2012 severalnines.com

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#include <iostream>
#include <fstream>
#include <my_global.h>
#include <mysql.h>
#include <my_config.h>
#include <m_ctype.h>


#include <ndb_types.h>

#include <NdbDictionary.hpp>
#include <NdbApi.hpp>

#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <getopt.h>

using namespace std;

#define OH_PK 20
#define PAGESIZE_DM 32768 
#define PAGESIZE_IM 8192
#define PAGESIZE_OH_DM 128 
#define PAGESIZE_OH_IM 0
#define OH_NULL_COLUMN 4
#define NO_OF_NULL_COLUMNS_PER_OH 32
#define OH_TUPLE 36
//#define OH_TUPLE_NULL 40
#define OH_TUPLE_DISK 40
#define OH_UNIQUE_INDEX 36
#define OH_ORDERED_INDEX 16
#define ALIGNMENT 4



static const char * columnTypeStr[] = {  
    "Undefined",
    "Tinyint",
    "Tinyunsigned",
    "Smallint",
    "Smallunsigned",
    "Mediumint",
    "Mediumunsigned",
    "Int",
    "Unsigned",
    "Bigint",
    "Bigunsigned",
    "Float",
    "Double",
    "Olddecimal",
    "Olddecimalunsigned",
    "Decimal",
    "Decimalunsigned",
    "Char",
    "Varchar",
    "Binary",
    "Varbinary",
    "Datetime",
    "Date",
    "Blob",
    "Text",
    "Bit",
    "Longvarchar",
    "Longvarbinary",
    "Time",
    "Year",
    "Timestamp"
};

static const char * storageTypeStr[] = {
      "StorageTypeMemory",
      "StorageTypeDisk"
    };




static const char * elementTypeStr[] = {
    "TypeUndefined",
    "SystemTable",
    "UserTable",
    "UniqueHashIndex",
    "",
    "",
    "OrderedIndex",
    "HashIndexTrigger",
    "IndexTrigger",
    "SubscriptionTrigger",
    "ReadOnlyConstraint",
    "TableEvent",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "Tablespace",
    "LogfileGroup",
    "Datafile",
    "Undofile",
    };

/**
 * prototype for pk_read function
 */


#define ERR(err)  printf("%s\n",err.message)
void list_tables(Ndb * ndb,  char * dbname);

int calc_var_column(const NdbDictionary::Table * t,
		  const NdbDictionary::Index * ix,
		  int col,
		  Ndb* ndb,
		  bool longvar,
		  bool ftScan,
          bool ignoreData);


int calc_blob_column(const NdbDictionary::Table * t,
		   const NdbDictionary::Index * ix,
		   int col,
		   Ndb* ndb,
		   int & szRam,
		   int & szDisk,
		   bool ftScan);

int calculate_dm(Ndb * ndb, 
		const NdbDictionary::Table * t,
		const NdbDictionary::Index * ix,
		int & bytesRam , 
		int & bytesDisk,
		bool ftScan,
		int & noOfBlobs,
        bool ignoreData,
        bool & varFound);


int calculate_dm_ui(Ndb * ndb, 
		    const NdbDictionary::Table * t,
		    const NdbDictionary::Index * ix,
		    int & bytesRam , 
		    int & bytesDisk,
		    bool ftScan,
		    int & noOfBlobs,
		    int & pk_cols,
            bool & varFound);


int supersizeme(Ndb * ndb,char * db, char * tbl, bool ftScan, bool ignoreData);


int select_count(Ndb* pNdb, const NdbDictionary::Table* pTab,
		 int parallelism,
		 int* count_rows,
		 NdbOperation::LockMode lock);










//#########################################################################################
//#########################################################################################
//#########################################################################################
void print_help()
{
  printf("  -? --help\t\t\tprints this information\n");
  printf("  -c --ndb-connectstring=<hostname>\thostname where ndb_mgmd is running, default 'localhost'\n");
  printf("  -d --database=<d>\t\tDatabase containing table to analyse.\n");
  printf("  -t --tablename=<t>\t\tTable to analyse. Default is ''\n");
  printf("  -a --analyze-full-table\t\tPerforms a full table scan to calculate the average data usage for variable size columns, and gives also a record count. Otherwise 1024 random records are analyzed.\n");
  printf("  -l --loadfactor\t\t x is the percentage (0-100) how much of the VAR* columns you expect to fill up (on average). This is if you don't have any sample data to base the average on. 100 is (default). If there are records in the table then this parameter will not be used.\n");
  printf("  -i --ignore-data\t\t Ignore records in tables if any and use laodfactor.\n");
  printf("  -v --verbose .\n");
  printf("\n");
}

static int  loadfactor=100;
static char ndbconnectstring[255];
static char database[255];
static char tablename[255];
static bool ftScan=false;
static bool ignoreData=false;

static char g_all_tables[20160][128];
static char g_all_dbs[20160][128];
static int  g_count=0;
static bool g_analyze_all=false;
static bool g_multi_db=false;
static bool verbose=false;

int option(int argc, char** argv)
{
  int c;
  
  while (1)
    {
      static struct option long_options[] =
	{
	  {"help", 0, 0, '?'},
	  {"database", 1, 0, 'd'},
	  {"tablename", 1, 0, 't'},
	  {"analyze-entire-table", 0, 0, 'a'},
	  {"loadfactor", 1, 0, 'l'},
	  {"ndb-connectstring", 1, 0, 'c'},
	  {"ignore-data", 0, 0, 0},
	  {"verbose", 0, 0, 0},
	  {0, 0, 0, 0}
	};
      /* getopt_long stores the option index here.   */
      int option_index = 0;

      c = getopt_long (argc, argv, "?ad:c:t:l:iv",
		       long_options, &option_index);

      /* Detect the end of the options.   */
      if (c == -1)
	{
	  break;
	}


      switch (c)
	{
	case 0:
	  /* If this option set a flag, do nothing else now.   */
	  if (long_options[option_index].flag != 0)
	    break;
	  printf ("option %s", long_options[option_index].name);
	  if (optarg)
	    printf (" with arg %s", optarg);
	  printf ("\n");
	  
	  break;
	case 'c':
	  memset( ndbconnectstring,0,255);
	  strcpy(ndbconnectstring,optarg);
	  break;
	case 'l':
	  loadfactor = atoi(optarg);
	  break;
	case 'd':
	  memset( database,0,255);
	  strcpy(database,optarg);
	  break;
	case 't':
	  memset( tablename,0,255);
	  strcpy(tablename,optarg);
	  break;
	case 'a':
	  ftScan=true;
	  break;
    case 'i':
      ignoreData=true;
      break;
    case 'v':
        verbose=true;
        break;
	case '?':
	  {
	    print_help();
	    exit(-1);
	    break;
	  }
	default:
	  printf("Wrong options given. Try '-?' for help\n");
	  exit(-1);
	  break;
	}
    }
  return 0;  
}  

int main(int argc, char ** argv)
{
  ndb_init();
  /**
   * define a connect string to the management server
   */
  

  memset( ndbconnectstring,0,255);
  memset( database,0,255);
  memset( tablename,0,255);
  strcpy(ndbconnectstring, "localhost:1186");
  strcpy(database, "");

  option(argc,argv);
  if(strcmp(tablename,"")==0)
    {
      g_analyze_all=true;
    }
  
  char * db;
  if(strcmp(database,"")==0)
    db=0;
  else
    db=database;

  char * table = tablename;

  /**
   * Create a Ndb_cluster_connection object using the connectstring
   */
  Ndb_cluster_connection * conn = new Ndb_cluster_connection(ndbconnectstring);


  /**
   * Connect to the management server
   * try 12 times, wait 5 seconds between each retry,
   * and be verbose (1), if connection attempt failes
   */
  if(conn->connect(12, 5, 1) != 0)
    {
      printf( "Unable to connect to management server." );
      return -1;
    }

  /**
   * Join the cluster
   * wait for 30 seconds for first node to be alive, and 0 seconds
   * for the rest.
   */
  if (conn->wait_until_ready(30,0) <0)
    {
      printf( "Cluster nodes not ready in 30 seconds." );
      return -1;
    }



  /**
   * The first thing we have to do is to instantiate an Ndb object.
   * The Ndb object represents a connection to a database.
   * It is important to note that the Ndb object is not thread safe!!
   * Thus, if it is a multi-threaded application, then typically each thread
   * uses its own Ndb object.
   *
   * Now we create an Ndb object that connects to the test database.
   */

  Ndb * ndb = new Ndb(conn);


  if (ndb->init() != 0)
    {
      /**
       * just exit if we can't initialize the Ndb object
       */
      return -1;
    }
  if(ftScan)
    {
      if(g_analyze_all)
	printf("Analyzing all tables. This may take a while.\n");
      else
	printf("Analyzing entire table. This may take a while.\n");
    }

  if (ignoreData) 
    {
       printf("record in database will be ignored.\n");
    }
 
  memset(g_all_tables,0,sizeof(g_all_tables));
  memset(g_all_dbs,0,sizeof(g_all_dbs));
  char filename[255];  
  if(g_analyze_all)
    {
      if(db!=0)
	sprintf(filename,"%s.csv",db); 
      else
	{
	  strcpy(filename,"all_databases.csv"); 
	  g_multi_db=true;
	}

      list_tables(ndb,db);
    }
  else
    {
      if(db==0)
	{
	  printf("You must specify the database when analyzing only one table\n");
	  exit(1);
	}
      g_count=1;
      sprintf(filename,"%s_%s.csv",db,table); 
      strcpy(g_all_tables[0], table);
    }
  FILE * fh =  fopen(filename,"w+");
  fclose(fh);
  for(int i=0;i<g_count;i++)
    {
      
      if(db!=0)
	{
	  if (supersizeme(ndb, db,  g_all_tables[i], ftScan,ignoreData ) <0)
	    return -1;
	}else
	{
	  if (supersizeme(ndb, g_all_dbs[i],  g_all_tables[i], ftScan,ignoreData) <0)
	    return -1;
	}
      printf("----------------------------------------------------\n");
    }

  return 0;

}


//Evaluation how many bites are waste due to ALIGNMENT
int waste(const NdbDictionary::Column * c)
{
  
  int rest=0;
  int wst=0;
 
  if((rest=(c->getSizeInBytes()) %  ALIGNMENT) > 0)
  {
      wst = (ALIGNMENT - rest);
      //if(c->getType()==NdbDictionary::Column::Tinyunsigned)
	  printf( "\t\tWARNING : wasting %d  bytes due to alignment \n",wst    );
  }
  return wst;
}


//Evaluation how many bites are waste due to ALIGNMENT
int waste_sz(int sz)
{
  
  int rest=0;
  int wst=0;
 
  if((rest=(sz %  ALIGNMENT)) > 0)
  {
      wst = (ALIGNMENT - rest);
      //if(c->getType()==NdbDictionary::Column::Tinyunsigned)
	  printf( "\t\tWARNING : wasting %d  bytes due to alignment \n",wst    );
  }
  return wst;
}



int calculate_dm(Ndb * ndb, 
		 const NdbDictionary::Table * t,
		 const NdbDictionary::Index * ix,
		 int & bytesRam , 
		 int & bytesDisk,
		 bool ftScan,
		 int & noOfBlobs,
         bool ignoreData,
         bool & varFound)

			    
{
  int no_var=0;
  bool isNullable=false;
  int noOfNullable=0;
  int cols = 0;
  int noOfBits=0;
  bytesRam=0;
  bytesDisk=0;
  cols= t->getNoOfColumns();

  varFound=false;
  // Gerald  : I comment this part and use next loop to find primary key if any
  //for(int i=0; i < t->getNoOfColumns() ; i++)
  //  {
  //    const NdbDictionary::Column *  c = t->getColumn(i);
  //    if(c->getPrimaryKey())
  //	{		  
  //	    pkFound=true;
  //	    //sizepk += (c->getSizeInBytes() + waste(c)); 
  //	    //printf( "pk = " <<  sizepk ) );
  //	}
  //  }
  
  for(int col=0; col < cols ; col++)
  {
      const NdbDictionary::Column * c = t->getColumn(col);
      int sz=0;      
      int disk_sz=0;
      const NdbDictionary::Column::Type type=c->getType();

      if (verbose) 
      {
          printf("Analysing column : %s, Type : %s,  size in bytes %d, storage : %s, is nullable %s\n",
                 c->getName(), columnTypeStr[type], c->getSizeInBytes(), storageTypeStr[c->getStorageType()],c->getNullable()? "true" : "false" );
      }

      // Check if attribut is a primary key
      if(c->getPrimaryKey())
      {	
        if (!strncmp(c->getName(),"$PK",3))
        {
          printf( "---\tWARNING! Primary key %s found, but looks like mysql cluster hidden PK (No primary key explicitly defined)!"
                  " Hidden PK cost is 8B (BigInt)!\n",c->getName());
        }
      }

      switch(type)
	  {
	    case NdbDictionary::Column::Tinyint:
	    case NdbDictionary::Column::Tinyunsigned:
	    case NdbDictionary::Column::Smallint:
	    case NdbDictionary::Column::Smallunsigned:
	    case NdbDictionary::Column::Mediumint:
	    case NdbDictionary::Column::Mediumunsigned:	  
	      printf("---\tWARNING! tiny/small/mediumint found. "
		         "If the column is not indexed consider replacing it with "
		         "BIT(%d)!!\n",8*c->getSizeInBytes() );
	      if (c->getStorageType() == NdbDictionary::Column::StorageTypeDisk)
	         disk_sz=(c->getSizeInBytes() + waste(c));
	      else
	         sz = (c->getSizeInBytes() + waste(c));       
	      break;


	    case NdbDictionary::Column::Bit:
	      noOfBits+=c->getLength();	  
	      break;

	    case NdbDictionary::Column::Varchar:
	    case NdbDictionary::Column::Varbinary:
	      if (c->getStorageType() == NdbDictionary::Column::StorageTypeDisk)
          {
            //VarXXX on disk have fixed sized
	        disk_sz=c->getSizeInBytes();
          }
	      else
	      {
	        sz=calc_var_column(t,0,col,ndb, false,ftScan,ignoreData);
	        no_var++;
	      }
	      break;

	    case NdbDictionary::Column::Longvarchar:
	    case NdbDictionary::Column::Longvarbinary:
	      if (c->getStorageType() == NdbDictionary::Column::StorageTypeDisk)
          {
            //VarXXX on disk have fixed sized
	        disk_sz=c->getSizeInBytes();
          }
	      else
	      {
	        sz=calc_var_column(t,0,col,ndb, true, ftScan,ignoreData);
	        no_var++;
	      }
	      break;

	    case NdbDictionary::Column::Blob:
	    case NdbDictionary::Column::Text:
	      // hack for now..
	      calc_blob_column(t,ix,col,ndb, sz, disk_sz, ftScan);       
	      noOfBlobs++;
	      break;

        default:
	      if (c->getStorageType() == NdbDictionary::Column::StorageTypeDisk)
	        disk_sz=c->getSizeInBytes()+ waste(c);       
	      else
	        sz = (c->getSizeInBytes() + waste(c));       
	      break;
	  
	  }

      bytesRam += sz;
      bytesDisk += disk_sz;
      
      if(c->getNullable())
      {
        isNullable=true;
        noOfNullable++;
      }
  }

  //Add Tuple overhead
  bytesRam += OH_TUPLE;

  if(isNullable)
  {
    if (verbose) 
    {
      printf("Overhead due to %d nullable column(s) : %d per rows\n", noOfNullable,
            ((noOfNullable / NO_OF_NULL_COLUMNS_PER_OH ) + (noOfNullable%NO_OF_NULL_COLUMNS_PER_OH == 0 ? 0:1)) * OH_NULL_COLUMN);
    }
    bytesRam = bytesRam + ((noOfNullable / NO_OF_NULL_COLUMNS_PER_OH ) + (noOfNullable%NO_OF_NULL_COLUMNS_PER_OH == 0 ? 0:1)) * OH_NULL_COLUMN;
  }

  int words32=0;
  if(noOfBits>0)
  { 
    words32=(noOfBits/32) + (noOfBits % 32 == 0 ? 0 : 1);
    if (verbose) 
    {
      printf("no of bits = %d, bits cost = %d, waste bit(s) = %d\n", noOfBits, words32*4, 32-(noOfBits % 32));
    }
  }  
  
  bytesRam += words32*4;

  bytesRam += (no_var+1)*2 /*TWO BYTES*/; 

  if (no_var>0) 
  {
    varFound=true; 
  }

  if(bytesDisk>0)
    bytesDisk+=OH_TUPLE_DISK;
  return 1;
}





int calculate_dm_ui(Ndb * ndb, 
		    const NdbDictionary::Table * t,
		    const NdbDictionary::Index * ix,
		    int & bytesRam , 
		    int & bytesDisk,
		    bool ftScan,
		    int & noOfBlobs,
		    int & pk_cols,
            bool & varFound)

			    
{
  int no_var=0;
  bool isNullable=false;
  int cols = 0;
  int noOfBits=0;
  bytesRam=0;
  bytesDisk=0;
  
  cols= ix->getNoOfColumns();
  /*
   * calculate size of PK if is a unique index
   */
  
  // Gerald  : I comment this part and use next loop to find primary key if any
  //for(int i=0; i < ix->getNoOfColumns() ; i++)
  //  {
  //    const NdbDictionary::Column *  c = ix->getColumn(i);
  //    if(c->getPrimaryKey())
  //	{		  
  //	  pkFound=true;
  //	  pk_cols++;
  //	}
  //    
  //  }
    
  for(int col=0; col < cols ; col++)
    {
      const NdbDictionary::Column * c=0;
      c = ix->getColumn(col);
      int col_maintable= (t->getColumn(c->getName()))->getColumnNo();
      int sz=0;      
      int disk_sz=0;
      const NdbDictionary::Column::Type type=c->getType();

      if (verbose) 
      {
          printf("Analysing column : %s, Type : %s,  size in bytes %d, storage : %s, is nullable %s\n",
                 c->getName(), columnTypeStr[type], c->getSizeInBytes(), storageTypeStr[c->getStorageType()],c->getNullable()? "true" : "false" );
      }

      // Check if attribut is a primary key
      if(c->getPrimaryKey())
      {	
        if (!strncmp(c->getName(),"$PK",3))
        {
          printf( "---\tWARNING! Primary key %s found, but looks like mysql cluster hidden PK (No primary key explicitly defined)!"
                  " Hidden PK cost is 8B (BigInt)!\n",c->getName());
        }
        pk_cols++;
      }

      switch(type)
	{
	case NdbDictionary::Column::Tinyint:
	case NdbDictionary::Column::Tinyunsigned:
	case NdbDictionary::Column::Smallint:
	case NdbDictionary::Column::Smallunsigned:
	case NdbDictionary::Column::Mediumint:
	case NdbDictionary::Column::Mediumunsigned:	  
	  printf("---\t\tWARNING! tiny/small/mediumint found. "
		 "If the column is not indexed consider replacing it with "
		 "BIT(%d)!!\n",8*c->getSizeInBytes() );
	  if (c->getStorageType() == NdbDictionary::Column::StorageTypeDisk)
	    disk_sz=(c->getSizeInBytes() + waste(c));
	  else
	    sz = (c->getSizeInBytes() + waste(c));       
	  
	  break;

	case NdbDictionary::Column::Bit:
	  noOfBits+=c->getLength();	  
	  break;

	case NdbDictionary::Column::Varchar:
	case NdbDictionary::Column::Varbinary:
	  if (c->getStorageType() == NdbDictionary::Column::StorageTypeDisk)
	    disk_sz=c->getSizeInBytes();
	  else
	    {
	      sz=calc_var_column(t,ix,col_maintable,ndb, false,ftScan,ignoreData);
	      no_var++;
	    }
	  break;

	case NdbDictionary::Column::Longvarchar:
	case NdbDictionary::Column::Longvarbinary:
	  if (c->getStorageType() == NdbDictionary::Column::StorageTypeDisk)
	    disk_sz=c->getSizeInBytes();
	  else
	    {
	      sz=calc_var_column(t,ix,col_maintable,ndb, true, ftScan,ignoreData);
	      no_var++;
	    }
	  break;

	case NdbDictionary::Column::Blob:
	case NdbDictionary::Column::Text:
	  // hack for now..
	  calc_blob_column(t,ix,col,ndb, sz, disk_sz, ftScan);       
	  noOfBlobs++;
	  break;

	default:
	  if (c->getStorageType() == NdbDictionary::Column::StorageTypeDisk)
	    disk_sz=c->getSizeInBytes()+ waste(c);       
	  else
	    sz = (c->getSizeInBytes() + waste(c));       
	  break;
	  
	}
      
      bytesRam += sz;
      bytesDisk += disk_sz;
      
      if(c->getNullable())
	isNullable=true;
    }
  
  bytesRam = bytesRam + OH_UNIQUE_INDEX;
  
  int words32=0;
  if(noOfBits>0)
  { 
    words32=(noOfBits/32) + (noOfBits % 32 == 0 ? 0 : 1);
    if (verbose) 
    {
      printf("no of bits = %d, bits cost = %d, waste bit(s) = %d\n", noOfBits, words32*4, 32-(noOfBits % 32));
    }
  }  

  bytesRam += words32*4;

  bytesRam += (no_var+1)*2 /*TWO BYTES*/; 

  if (no_var>0) 
  {
    varFound=true; 
  }
    
  if(bytesDisk>0)
    bytesDisk+=OH_TUPLE_DISK;
  return 1;
}

			    

int supersizeme(Ndb * ndb,char * db, char * tbl, bool ftScan, bool ignoreData)
{
  bool varFound;
  bool varFoundui;
  int dm_per_rec=0;
  int im_per_rec=0;
  int disk_per_rec=0;
  int noOfOrderedIndexes=0, noOfUniqueHashIndexes=0, noOfBlobs=0;
  int tmpDm=0,tmpIm=0, tmpDisk=0;
  ndb->setDatabaseName(db);
  NdbDictionary::Dictionary * dict  = ndb->getDictionary();
  const NdbDictionary::Table * table  = dict->getTable(tbl);
  if(table == 0)
    {
      printf( "table %s in database %s not found!\n", tbl,db);
      return -1;
    }

  bool isTable=false;
  
  
  printf("\nCalculating storage cost per record for table %s\n", table->getName());
  
  calculate_dm( ndb, table, NULL, tmpDm, tmpDisk, ftScan, noOfBlobs,ignoreData,varFound );
  // Gerald there is at least 1 PK (hidden or real) and not return by listIndexes()
  // So add according OH + increment noOfUniqueHashIndexes
  tmpIm = OH_PK;
  noOfUniqueHashIndexes++;
  
  dm_per_rec +=tmpDm;
  disk_per_rec +=tmpDisk;
  im_per_rec +=tmpIm;

  NdbDictionary::Dictionary::List list;
  dict->listIndexes(list, *table);   
  int no_attrs=table->getNoOfColumns();
  for (unsigned i = 0; i < list.count; i++) 
  {      
    NdbDictionary::Dictionary::List::Element& elt = list.elements[i];

    if (verbose) 
    {
        printf("Analysing element : %s, Type : %s \n",
               elt.name, elementTypeStr[elt.type] );
    }
    switch (elt.type) 
    {
      case NdbDictionary::Object::UniqueHashIndex:
	    {
	      const NdbDictionary::Index * ix  = dict->getIndex(elt.name, table->getName());
	      printf(  "---\tWARNING! Unique Index found named (\"%s\"): \n",elt.name);
	      int pk_cols=0;
	      calculate_dm_ui(ndb, table, ix, tmpDm, tmpDisk, ftScan, noOfBlobs, pk_cols,varFoundui);
	      printf(  "---\t\tUnique Index Cost -  DataMemory per record = %d and IndexMemory  = %d\n", tmpDm, tmpIm );
	      //Gerald : OH_PK already include and OH_UNIQUE8HASH_INDEX is included by calculate_dm_ui
          // tmpIm = OH_PK;
	      dm_per_rec += tmpDm;
	      disk_per_rec += tmpDisk;
	      im_per_rec += tmpIm;
	      isTable = true;
	      noOfUniqueHashIndexes++;
	      //no_attrs+=(ix->getNoOfColumns()+pk_cols);
        }
        break;

      case NdbDictionary::Object::OrderedIndex:
	    tmpDm = OH_ORDERED_INDEX;
	    tmpIm = 0;
	    printf(  "---\tOrdered Index found named (%s"
		         "). Additional cost per record is = %d" 
		         " bytes of DataMemory.\n",
		         elt.name, tmpDm  );
	    dm_per_rec += tmpDm;
	    isTable = true;
	    noOfOrderedIndexes++;
	    break;

      default: 
	    break;
    }
  }


  int rows = 0;
  
  if (select_count(ndb, table, 240, &rows, 
		   NdbOperation::LM_CommittedRead) <0){
    printf( "counting rows failed\n" );
    return 0;
  }
  
  printf("\nRecord size (incl OH):"
	 "\n\t#Rows found=%d records "  
	 "\n\t#OrderedIndexes=%d"  
	 "\n\t#UniqueHashIndexes=%d "  
	 "\n\t#blob/text=%d "  
	 "\n\t#attributes=%d "  
	 "\n\tDataMemory=%d bytes "  
	 "\n\tIndexMemory=%d bytes" 
	 "\n\tDiskspace=%d bytes\n\n",
	 rows,
	 noOfOrderedIndexes,
	 noOfUniqueHashIndexes,
	 noOfBlobs,
	 no_attrs,
	 dm_per_rec, 
	 im_per_rec,
	 disk_per_rec);

  printf("\n\nAppending the following to %s.csv \n",db);
  printf("%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n\n",
	 db,
	 table->getName(), 
	 rows,
	 1,
	 noOfOrderedIndexes,
	 noOfUniqueHashIndexes,
	 noOfBlobs,
	 no_attrs,
	 im_per_rec,
	 dm_per_rec,
	 disk_per_rec,
     varFound ? 1:0,
     varFoundui ? 1:0);


  
  char filename[255];  
  if(g_analyze_all)
    {
      if(!g_multi_db)
	sprintf(filename,"%s.csv",db); 
      else	
	strcpy(filename,"all_databases.csv"); 
    }
  else
    sprintf(filename,"%s_%s.csv",db,tbl); 

  
  FILE * fh =  fopen(filename,"a+");
  char row[128];
  sprintf(row, "%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n" ,	  
	  db ,
	  table->getName() ,
	  rows ,
	  1,
	  noOfOrderedIndexes,
	  noOfUniqueHashIndexes,
	  noOfBlobs,
	  no_attrs,
	  im_per_rec ,
	  dm_per_rec ,
	  disk_per_rec,
      varFound ? 1:0,
      varFoundui ? 1:0);
  fwrite(row, strlen(row),1, fh);
  fclose(fh);


  return 1;
}

int calc_var_column(const NdbDictionary::Table * t,
		    const NdbDictionary::Index * ix,
		    int col,
		    Ndb* ndb,
		    bool longvarchar,
		    bool ftScan,
            bool ignoreData)
{
  
  char buff[8052];
  memset(buff, 0, sizeof(buff));
  int sz=0;
  NdbTransaction * trans = ndb->startTransaction();
  if(trans==0)
    abort();

  NdbScanOperation * sop;
  sop=trans->getNdbScanOperation(t);

  sop->readTuples();
  NdbRecAttr * attr=sop->getValue(col, buff);
  int rows=0;
  int charset_size=1;
  bool no_data=false;  

  //Set charset cost (for binary and latin1 cost is 1)
  const NdbDictionary::Column  * c = t->getColumn(col);
  const NdbDictionary::Column::Type type = c->getType();
  if ((type == NdbDictionary::Column::Varchar) || (type == NdbDictionary::Column::Longvarchar))
  {
    CHARSET_INFO * cs2;
    cs2= (CHARSET_INFO*)(c->getCharset());

    if(cs2!=0)
    {       
      if(strncmp(cs2->name, "utf8",4)==0)
      {
        charset_size=3;
        printf(  "---\tWARNING! cs2->name charset used, each character cost : %d bytes\n",charset_size);
      }

      if(strncmp(cs2->name, "ucs2",4)==0)
      {
        charset_size=2;
        printf(  "---\tWARNING! cs2->name charset used, each character cost : %d bytes\n",charset_size);
      }
    }
  }

  //Set var header size
  int headerSize=longvarchar ? 2 : 1;



  if(trans->execute(NdbTransaction::NoCommit, 
		    NdbOperation::AbortOnError, 1) == -1)
  {
	no_data=true;
	trans->close();
  }


  if(!no_data)
  {
    int check=0;
    while( ((check = sop->nextResult(true)) == 0) && !ignoreData)
	{  
	  rows++;
      if (verbose) 
      {
        printf("attribut %d size = %d \n",rows,attr->isNULL()==1 ? 0 : attr->get_size_in_bytes());  
      }


      if(attr->isNULL()==1)  
      {
        sz+=0; //for the time being until we know for sure..
      }
      else
      { //attr->get_size_in_bytes return lengh of attr including header attr cost = (length-header*charset)
        sz+=((attr->get_size_in_bytes() - headerSize)* charset_size);
      }
            
	  if(rows==1024 && !ftScan)
	    break;
	}
    trans->close();
  }

  if(rows==0)
  {
    sz = (int)(((float)(t->getColumn(col)->getSizeInBytes()))* (float)((float)loadfactor/100));
    printf("---\tWARNING! No reference data found for VAR*. Defaulting to max size (loadfactor=100 percent)..%d bytes \n",sz);
    printf("\tConsider loading database with average data for exact measurement\n");
      
    return sz+waste_sz(sz);
  }

  int tmpsz=(sz/rows)+headerSize;
  sz=tmpsz + waste_sz(tmpsz);
  if(ix)
    printf("---\t\tVAR* attribute is %d bytes averaged over %d rows\n", sz, rows);
  else
    printf("---\tVAR* attribute is %d bytes averaged over %d rows\n", sz, rows);
  return sz;
}




int calc_blob_column(const NdbDictionary::Table * t,
		   const NdbDictionary::Index * ix,
		   int col,
		   Ndb* ndb,
		   int & szRam,
		   int & szDisk,
		   bool ftScan)
{

  NdbTransaction * trans = ndb->startTransaction();
  NdbScanOperation * sop = trans->getNdbScanOperation(t);

  sop->readTuples();

  NdbBlob * blob = sop->getBlobHandle(col);  
  bool no_data=false;
  if(trans->execute(NdbTransaction::NoCommit, 
		    NdbOperation::AbortOnError, 1) == -1)
    {
      no_data=true;
      sop->close();
      trans->close();
    }
  
  unsigned long long len=0;


  int rows=0;
  int check=0;
  const NdbDictionary::Column *  c = t->getColumn(col);
  int part_size= c->getPartSize();
  if(!no_data)
    {
      while(((check = sop->nextResult(true)) == 0) && !ignoreData)
	{
	  int isnull;
	  rows++;
	  blob->getNull(isnull);
	  if(isnull)
	    len=0;
	  else	    
	    blob->getLength(len);
	  
	  /*	  
		  printf("blob is %llu\n", len);
		  if(len>256)
		  {
		  szRam+=(((len-256)/part_size) + 1)*part_size+256;
		  printf("len2=%llu, part-size=%d, len=%llu\n", (((len-256)/part_size) + 1)*part_size+256, part_size, len);
		  
		  }
		  else
	  */
	  
	  szRam+=(int)len;
	  if(rows==1000 && !ftScan)
	    break;
	}
      sop->close();
      trans->close();
    }
  if(rows==0)
    {
      if (c->getStorageType() == NdbDictionary::Column::StorageTypeDisk)
      {
          printf("---\tWARNING! No reference data found for BLOB/TEXT. "
             "Defaulting to 256 bytes DataMemory, %d bytes Diskspace! \n",(part_size<=256 ? 0:part_size));
          printf("\tConsider loading database with average data for exact"
             " measurement. \n");
          szRam=256;
          szDisk=(part_size<=256 ? 0:part_size);
          return 0;
      }
      else
      {
         printf("---\tWARNING! No reference data found for BLOB/TEXT. "
	        "Defaulting to %d bytes DataMemory ! \n", (part_size<=256 ? 256:part_size+256));
         printf("\tConsider loading database with average data for exact"
	        " measurement. \n");
         szRam=(part_size<=256 ? 256:part_size+256);
         szDisk=0;
         return 0;
      }
    }
  if (c->getStorageType() == NdbDictionary::Column::StorageTypeDisk)
    {
    int averageSz=szRam/rows;
    if((averageSz)>256)
	  {
      szRam=256;
	  szDisk=((averageSz-256)/part_size) *part_size + (((averageSz-256)%part_size)==0 ? 0:part_size);
	  }
      else
	  {
      szRam=256;
	  szDisk=0;
	  }
    }
  else
    {
      int averageSz=szRam/rows;
      szDisk=0;
      if((averageSz)<256)
        {
        szRam=256;
        }
        else
        {
        szRam=256 + ((averageSz-256)/part_size)*part_size + (((averageSz-256)%part_size)==0 ? 0:part_size);
        }
    }
   
  printf("---\tBLOB/TEXT attribute is %d bytes (RAM) and %d bytes (DISK)"
	 " averaged over %d rows\n", 	 
	 szRam, 
	 szDisk,
	 rows);

  return 0;
}



int 
select_count(Ndb* pNdb, const NdbDictionary::Table* pTab,
	     int parallelism,
	     int* count_rows,
	     NdbOperation::LockMode lock){
  
  int                  retryAttempt = 0;
  const int            retryMax = 100;
  int                  check;
  NdbTransaction       *pTrans;
  NdbScanOperation	       *pOp;
  const Uint32 codeWords= 1;
  Uint32 codeSpace[ codeWords ];
  NdbInterpretedCode code(NULL, // Table is irrelevant
                          &codeSpace[0],
                          codeWords);
  if ((code.interpret_exit_last_row() != 0) ||
      (code.finalise() != 0))
  {
    ERR(code.getNdbError());
    return -1;
  }

  while (true){

    if (retryAttempt >= retryMax){
      printf( "ERROR: has retried this operation %d times, failing!\n",
	      retryAttempt );
      return -1;
    }

    pTrans = pNdb->startTransaction();
    if (pTrans == NULL) {
      const NdbError err = pNdb->getNdbError();

      if (err.status == NdbError::TemporaryError){
	usleep(50);
	retryAttempt++;
	continue;
      }
      ERR(err);
      return -1;
    }
    pOp = pTrans->getNdbScanOperation(pTab->getName());	
    if (pOp == NULL) {
      ERR(pTrans->getNdbError());
      pNdb->closeTransaction(pTrans);
      return -1;
    }

    if( pOp->readTuples(NdbScanOperation::LM_Dirty) ) {
      ERR(pTrans->getNdbError());
      pNdb->closeTransaction(pTrans);
      return -1;
    }


    check = pOp->setInterpretedCode(&code);
    if( check == -1 ) {
      ERR(pTrans->getNdbError());
      pNdb->closeTransaction(pTrans);
      return -1;
    }
  
    Uint64 tmp;
    Uint32 row_size;
    pOp->getValue(NdbDictionary::Column::ROW_COUNT, (char*)&tmp);
    pOp->getValue(NdbDictionary::Column::ROW_SIZE, (char*)&row_size);
    check = pTrans->execute(NdbTransaction::NoCommit);
    if( check == -1 ) {
      ERR(pTrans->getNdbError());
      pNdb->closeTransaction(pTrans);
      return -1;
    }
    
    Uint64 row_count = 0;
    int eof;
    while((eof = pOp->nextResult(true)) == 0){
      row_count += tmp;
    }
    
    if (eof == -1) {
      const NdbError err = pTrans->getNdbError();
      
      if (err.status == NdbError::TemporaryError){
	pNdb->closeTransaction(pTrans);
	usleep(50);
	retryAttempt++;
	continue;
      }
      ERR(err);
      pNdb->closeTransaction(pTrans);
      return -1;
    }
    
    pNdb->closeTransaction(pTrans);
    
    if (count_rows != NULL){
      *count_rows = row_count;
    }
    
    return 1;
  }
  return -1;
}

void list_tables(Ndb * ndb,  char * dbname)
{
  NdbDictionary::Dictionary * dic=ndb->getDictionary();
  NdbDictionary::Dictionary::List list;
  NdbDictionary::Object::Type type=NdbDictionary::Object::UserTable;
  if (dic->listObjects(list, type) == -1)
    exit(-1);
  for (unsigned i = 0; i < list.count; i++) {
    NdbDictionary::Dictionary::List::Element& elt = list.elements[i];
    switch (elt.type) {
     case NdbDictionary::Object::UserTable:
       if(dbname!=0)
	 {
	   if(strcmp(elt.database,dbname)==0 && strncmp(elt.name,"NDB$BLOB",8)!=0)
	     {
	       strcpy(g_all_tables[g_count], elt.name);
	       g_count++;
	     }
	 }
       else if(strncmp(elt.name,"NDB$BLOB",8)!=0)
	 {
	   strcpy(g_all_tables[g_count], elt.name);
	   strcpy(g_all_dbs[g_count], elt.database);
	   g_count++;
	 }
      break;
    default:
      break;      
    }
  }
}
