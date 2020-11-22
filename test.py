#!/usr/bin/python
# A Simple Python Program(Compatible with python 3+) to retrive all the statistics of the Postgres DB.
# How to Use?
# 1. Directly using Python with the required packages installed.
#    python3 pgreview.py -h <host1.hostname.com> -d <databasename> -U <username>
#
# 2. Building the docker images and running via Docker.
#    docker run -it <imagename> -h <host1.hostname.com> -d <databasename> -U <username>
import sys,re,datetime,argparse,psycopg2
import getpass
from tabulate import tabulate
from termcolor import colored
# sys.setdefaultencoding('utf8')
now = datetime.datetime.now()
parser = argparse.ArgumentParser(description='PostgreSQL Review Script',
    epilog='Example 1(non-docker):\n %(prog)s -h hostanme -d dbname -U username\nExample 2(docker):\n docker run -it <imagename> -h hostanme -d dbname -U username\n',
    formatter_class=argparse.RawTextHelpFormatter,
    conflict_handler="resolve")
parser.add_argument('-h','--host',help="Hostname",required=True)
parser.add_argument('-d','--dbname',help="database name to connect to",required=True)
parser.add_argument('-U','--username', help="database user name",required=True)
parser.add_argument('-f','--file',help="Redirect the output to the given file and also to the STDOUT")
if len(sys.argv)==1:
    parser.print_help()
    sys.exit(1)
args = parser.parse_args()
password = getpass.getpass("Password: ")
#Establish connection to database and handle exception
def create_conn():
    print("Connecting to Databse...\n")
    try:
       # conn = psycopg2.connect(args.connection+" password="+password+" connect_timeout=10")
      conn = psycopg2.connect("host="+args.host+" dbname="+args.dbname+" user="+args.username+" password="+password+" connect_timeout=10")
    except psycopg2.Error as e:
       print(colored ("Unable to connect to database :", 'red'))
       print(e)
       sys.exit(1)
    return conn
# Parsing the Hostname
def hostparse():
    hostname=args.host
    print("Hostname" + " "*15 + ": " + hostname)
#close the connection
def close_conn(conn):
    print(colored("Closing the connection...",'red'))
    conn.close()
# Accepts a DB connection or cursor and executes the Query
def query(cursor, sql, params=None):
    """accepts a database connection or cursor"""
    if type(cursor) == psycopg2._psycopg.connection:
        cursor = cursor.cursor()
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        results = cursor.fetchall()
    except psycopg2.Error as e:
        print (e.pgerror)
        results = []
    cursor.close()
    return results
# Extract the DB version
def dbversion(conn):
    sql = ("SHOW server_version")
    results = query(conn, sql)
    print("PostgreSQL version"+ " "*5 + ": " + str(results[0][0]))
# Extract the DB name and Size
def dbNameSize(conn):
    sql = ("SELECT datname, pg_size_pretty(pg_database_size(datname)) FROM pg_database WHERE datname = current_database()")
    results = query(conn, sql)
    print ("Database Name"+ " "*10 + ": " + str(results[0][0]))
    print ("Database Size"+ " "*10 + ": " + str(results[0][1]))
# Extract the DB Uptime
def dbUptime(conn):
    sql = ("SELECT current_timestamp - pg_postmaster_start_time()")
    results = query(conn, sql)
    print ("Uptime" + " "*(24-7) + ": " + str(results[0][0]))
#Find the Schemas Present in the current DB
def dbschema(conn):
    sql = ('''select nspname
              from pg_catalog.pg_namespace
              where nspname not in ('pg_catalog','information_schema') and nspname not like 'pg_toast%' and nspname not like 'pg_temp%' ''')
    #print sql
    results = query(conn, sql)
    if not results or None in results[0]:
        print ("\nUser defined Schemas in the DB are"+ " "*18 + ": None")
    else:
        print("\nUser defined Schemas in the DB are"+ " "*18 + ": {}".format(' ,'.join(map(str,results)).replace("('","").replace("',)","")))
        return results
#Check for replication if any
def dbreplicacheck(conn):
    sql = ("select usename,client_addr,pg_xlog_location_diff(pg_current_xlog_location(), replay_location) AS bytes_diff from pg_stat_replication")
    if conn.server_version >= 100000: # PostgreSQL 10 and higher
        sql = sql.replace('_xlog', '_wal')
        sql = sql.replace('_location', '_lsn')
    results = query(conn, sql)
    if not results:
        print(colored ("Replication"+" "*12 + ": Not Available"))
    else:
        print ("Replication"+" "*12 + ": configured")
        all_delays = []
        if None in results[0]:
            print(colored ("Replication Details    : User used does not have superuser rights to fetch details of replication ","red"))
        else:
            for result_row in results:
                print(colored ("Replication Details below   :"))
                print ("    Client Address" +" "*12 +": " +result_row[1])
                print ("    Bytes different from master : "+str(int(result_row[2])))
# Extract the table without Primary Key
def dbtbwoprimarykey(conn):
    sql = ('''SELECT t.nspname, t.relname FROM
          (SELECT c.oid, c.relname, n.nspname FROM
              pg_class c
              JOIN
              pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relkind = 'r' AND n.nspname IN('public') ) AS t
              LEFT OUTER JOIN
              pg_constraint c ON c.contype = 'p' AND c.conrelid = t.oid WHERE c.conname IS NULL''')
    results = query(conn, sql)
    if not results or None in results[0]:
        print ("\nTables without Primary Key: 0")
    else:
        print(colored ("\nTables without Primay Key are :","green"))
        print(tabulate (results,headers=['SchemaName','TableName'], tablefmt='fancy_grid'))
    results= []
# Extract the Index Hit ratio
def index_hitratio(conn):
    sql = ("SELECT ((sum(idx_blks_hit)) / nullif(sum(idx_blks_hit + idx_blks_read),0))*100 AS index_hit_rate FROM pg_statio_user_indexes")
    results = query(conn, sql)
    print('The Index Hit Rate is            : %.5s %%' %(results[0]))
# Extract the table Hit ratio
def table_hitratio(conn):
    sql = ("SELECT (sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read),0))*100 AS table_hit_rate FROM pg_statio_user_tables")
    results = query(conn, sql)
    print('The Table Hit Rate is            : %.5s %%' %(results[0]))
# Extract all the unused Indexes
def unusedidx(conn):
    sql =("select schemaname,relname,indexrelname,indisprimary from pg_stat_user_indexes ps,pg_index pi where ps.indexrelid=pi.indexrelid and schemaname not like 'pg%' and idx_scan = 0")
    results = query(conn, sql)
    if not results or None in results[0]:
        print ("\nUnused Indexes are            : None")
    else:
        print(colored ("\nUnused Indexes are:","green"))
        print(tabulate (results,headers=['SchemaName','TableName', 'IndexName','PrimaryKey(True/False)'], tablefmt='fancy_grid'))
    results= []
#Find the Temporary file used
def tempfile():
    sql =("SELECT temp_files, temp_bytes FROM pg_stat_database WHERE datname = current_database()")
    results = query(conn, sql)
    if not results or None in results[0]:
        print ("No temp files are"+" "*12 +": None")
    else:
        print(colored ("\nStats of Temp Files(Post last restart):","green"))
        print(tabulate (results,headers=['TemporaryFiles', 'DataWrittenToTempFiles( bytes)'], tablefmt='fancy_grid'))
# Find the configured max_connection
def maxconnection(conn):
    sql = ("show max_connections")
    results = query(conn, sql)
    print ('Max connections(max_connections) '+ ' '*15 +": " + str(results[0][0]))
def AutovacuumAnalyzeScaleFactor(conn):
    sql = ("show autovacuum_analyze_scale_factor")
    results = query(conn, sql)
    print ('autovacuum_analyze_scale_factor'+ ' '*18 +": " + str(results[0][0]))
def AutovacuumScaleFactor(conn):
    sql = ("show autovacuum_vacuum_scale_factor")
    results = query(conn, sql)
    print ('autovacuum_vacuum_scale_factor'+ ' '*17 +": " + str(results[0][0]))
#Find the configured shared_buffers
def sharedbuff(conn):
    sql = ("show shared_buffers")
    results = query(conn, sql)
    print ('Shared Buffer(shared_buffer)' +' '*20 +": " + str(results[0][0]))
#Find the configured work_mem
def workmem(conn):
    sql =("show work_mem")
    results = query(conn, sql)
    print ('Work Memory(work_mem)' + ' '*27 + ": " +  str(results[0][0]))
#Find the configured maintenance_work_mem
def maintenanceworkmem(conn):
    sql =("show maintenance_work_mem")
    results = query(conn, sql)
    print ("Maintenance Work Memory (maintenance_work_mem)  : " + str(results[0][0]))
#Check if there any specific configs for autovacuum for tables
def tableautovacuumconfig(conn):
    sql = ("select relname, reloptions from pg_class where relname not like 'pg_%' and reloptions is not null")
    results = query(conn, sql)
    if not results or None in results[0]:
        print ("\nTable Level autovacuum configs   : None")
    else:
        print(colored ("\nTable Level autovacuum configs are as below:","green"))
        print(tabulate(results,headers=['Table Name','Config'], tablefmt='fancy_grid'))
#Find the configured effective_cache_size
def effcachesize(conn):
    sql = ("show effective_cache_size")
    results = query(conn, sql)
    print ("Cache available in kernel (effective_cache_size): " + str(results[0][0]))
#Find if autovacuum is enabled
def autovacuumsetting(conn):
    sql = ("show autovacuum")
    results = query(conn, sql)
    print ("Auto Vacuuming (autovacuum)" + " "*21 + ": " + str(results[0][0]))
#Find if there are invalid indexes
def invalidIdx(conn):
    sql = ("select relname from pg_index join pg_class on indexrelid=oid where indisvalid=false")
    results = query(conn, sql)
    if not results or None in results[0]:
        print ("Invalid Indexes"+" "*18 +": None")
    else:
        print(colored ("Invalid Indexes are              :", "green")),
        print(tabulate(results,headers=['Invalid Indexes'], tablefmt='fancy_grid'))
    results= []
# Vacuum details
def vacuumdetails(conn):
    sql = (''' WITH rel_set AS
             (
              SELECT
                oid,
                CASE split_part(split_part(array_to_string(reloptions, ','), 'autovacuum_vacuum_threshold=', 2), ',', 1)
                WHEN '' THEN NULL
                ELSE split_part(split_part(array_to_string(reloptions, ','), 'autovacuum_vacuum_threshold=', 2), ',', 1)::BIGINT
                END AS rel_av_vac_threshold,
                CASE split_part(split_part(array_to_string(reloptions, ','), 'autovacuum_vacuum_scale_factor=', 2), ',', 1)
                WHEN '' THEN NULL
                ELSE split_part(split_part(array_to_string(reloptions, ','), 'autovacuum_vacuum_scale_factor=', 2), ',', 1)::NUMERIC
                END AS rel_av_vac_scale_factor
                FROM pg_class
                )
              SELECT
                PSUT.relname,
                to_char(PSUT.last_vacuum, 'YYYY-MM-DD HH24:MI')     AS last_vacuum,
                to_char(PSUT.last_autovacuum, 'YYYY-MM-DD HH24:MI') AS last_autovacuum,
                to_char(C.reltuples, '9G999G999G999')               AS n_tup,
                to_char(PSUT.n_dead_tup, '9G999G999G999')           AS dead_tup,
                to_char(coalesce(RS.rel_av_vac_threshold,
                                current_setting('autovacuum_vacuum_threshold')::BIGINT) + coalesce(RS.rel_av_vac_scale_factor,
                                current_setting('autovacuum_vacuum_scale_factor')::NUMERIC) * C.reltuples, '9G999G999G999') AS av_threshold,
                CASE
                WHEN (coalesce(RS.rel_av_vac_threshold, current_setting('autovacuum_vacuum_threshold')::BIGINT) + coalesce(RS.rel_av_vac_scale_factor,
                current_setting('autovacuum_vacuum_scale_factor')::NUMERIC) * C.reltuples) < PSUT.n_dead_tup
                THEN '*'
                ELSE ''
                END AS expect_av,
                vacuum_count,
                autovacuum_count
                FROM
                pg_stat_user_tables PSUT
                JOIN pg_class C
                ON PSUT.relid = C.oid
                JOIN rel_set RS
                ON PSUT.relid = RS.oid
                ORDER BY C.reltuples DESC''')
    results = query(conn, sql)
    print(colored ("\nThe Vacumm Details are given below:","green"))
    print(tabulate(results,headers=['TableName', 'LastVacuum', 'LastAutovacuum','RowCount','DeadRows','AvgThresholdForNxtVacuum','ExpectAvg','VacuumCount','AutovacuumCount'], tablefmt='fancy_grid'))
    results= []
# Find tables which are bloated
def bloatinfo(conn):
    sql='''WITH constants AS (
        -- define some constants for sizes of things
        -- for reference down the query and easy maintenance
            SELECT current_setting('block_size')::numeric AS bs, 23 AS hdr, 8 AS ma
        ),
        no_stats AS (
            -- screen out table who have attributes
            -- which dont have stats, such as JSON
            SELECT table_schema, table_name,
                n_live_tup::numeric as est_rows,
                pg_table_size(relid)::numeric as table_size
            FROM information_schema.columns
                JOIN pg_stat_user_tables as psut
                ON table_schema = psut.schemaname
                AND table_name = psut.relname
                LEFT OUTER JOIN pg_stats
                ON table_schema = pg_stats.schemaname
                    AND table_name = pg_stats.tablename
                    AND column_name = attname
            WHERE attname IS NULL
                AND table_schema NOT IN ('pg_catalog', 'information_schema')
            GROUP BY table_schema, table_name, relid, n_live_tup
        ),
        null_headers AS (
            -- calculate null header sizes
            -- omitting tables which dont have complete stats
            -- and attributes which aren't visible
            SELECT
                hdr+1+(sum(case when null_frac <> 0 THEN 1 else 0 END)/8) as nullhdr,
                SUM((1-null_frac)*avg_width) as datawidth,
                MAX(null_frac) as maxfracsum,
                schemaname,
                tablename,
                hdr, ma, bs
            FROM pg_stats CROSS JOIN constants
                LEFT OUTER JOIN no_stats
                    ON schemaname = no_stats.table_schema
                    AND tablename = no_stats.table_name
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                AND no_stats.table_name IS NULL
                AND EXISTS ( SELECT 1
                    FROM information_schema.columns
                        WHERE schemaname = columns.table_schema
                            AND tablename = columns.table_name )
            GROUP BY schemaname, tablename, hdr, ma, bs
        ),
        data_headers AS (
            -- estimate header and row size
            SELECT
                ma, bs, hdr, schemaname, tablename,
                (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
                (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
            FROM null_headers
        ),
        table_estimates AS (
            -- make estimates of how large the table should be
            -- based on row and page size
            SELECT schemaname, tablename, bs,
                reltuples::numeric as est_rows, relpages * bs as table_bytes,
            CEIL((reltuples*
                    (datahdr + nullhdr2 + 4 + ma -
                        (CASE WHEN datahdr%ma=0
                            THEN ma ELSE datahdr%ma END)
                        )/(bs-20))) * bs AS expected_bytes,
                reltoastrelid
            FROM data_headers
                JOIN pg_class ON tablename = relname
                JOIN pg_namespace ON relnamespace = pg_namespace.oid
                    AND schemaname = nspname
            WHERE pg_class.relkind = 'r'
        ),
        estimates_with_toast AS (
            -- add in estimated TOAST table sizes
            -- estimate based on 4 toast tuples per page because we dont have
            -- anything better.  also append the no_data tables
            SELECT schemaname, tablename,
                TRUE as can_estimate,
                est_rows,
                table_bytes + ( coalesce(toast.relpages, 0) * bs ) as table_bytes,
                expected_bytes + ( ceil( coalesce(toast.reltuples, 0) / 4 ) * bs ) as expected_bytes
            FROM table_estimates LEFT OUTER JOIN pg_class as toast
                ON table_estimates.reltoastrelid = toast.oid
                    AND toast.relkind = 't'
        ),
        table_estimates_plus AS (
        -- add some extra metadata to the table data
        -- and calculations to be reused
        -- including whether we cant estimate it
        -- or whether we think it might be compressed
            SELECT current_database() as databasename,
                 schemaname, tablename, can_estimate,
                    est_rows,
                    CASE WHEN table_bytes > 0
                        THEN table_bytes::NUMERIC
                        ELSE NULL::NUMERIC END
                        AS table_bytes,
                    CASE WHEN expected_bytes > 0
                        THEN expected_bytes::NUMERIC
                        ELSE NULL::NUMERIC END
                            AS expected_bytes,
                    CASE WHEN expected_bytes > 0 AND table_bytes > 0
                        AND expected_bytes <= table_bytes
                        THEN (table_bytes - expected_bytes)::NUMERIC
                        ELSE 0::NUMERIC END AS bloat_bytes
            FROM estimates_with_toast
            UNION ALL
            SELECT current_database() as databasename,
                table_schema, table_name, FALSE,
                est_rows, table_size,
                NULL::NUMERIC, NULL::NUMERIC
            FROM no_stats
        ),
        bloat_data AS (
            -- do final math calculations and formatting
            select current_database() as databasename,
                schemaname, tablename, can_estimate,
                table_bytes, round(table_bytes/(1024^2)::NUMERIC,3) as table_mb,
                expected_bytes, round(expected_bytes/(1024^2)::NUMERIC,3) as expected_mb,
                round(bloat_bytes*100/table_bytes) as pct_bloat,
                round(bloat_bytes/(1024::NUMERIC^2),2) as mb_bloat,
                table_bytes, expected_bytes, est_rows
            FROM table_estimates_plus
        )
        -- filter output for bloated tables
        SELECT databasename, schemaname, tablename,
            can_estimate,
            est_rows,
            pct_bloat, mb_bloat,
            table_mb
        FROM bloat_data
        -- this where clause defines which tables actually appear
        -- in the bloat chart
        -- example below filters for tables which are either 50%
        -- bloated and more than 20mb in size, or more than 25%
        -- bloated and more than 1GB in size
        WHERE ( pct_bloat >= 50 AND mb_bloat >= 20 )
            OR ( pct_bloat >= 25 AND mb_bloat >= 1000 )
        ORDER BY pct_bloat DESC;'''
    results = query(conn,sql)
    if not results or None in results[0]:
        print(colored ("\nBloated Tables Info :   There are No Bloated Tables (Run ANALYSE for recent stats )","green"))
    else:
        print(colored ("\nBloated Table info below: (Recent ANALYSE of the table will give accurate stats )","green"))
        print(tabulate(results,headers=['DBName','SchemaName','TableName','Can_Estimate','Est_Rows','Pct_Bloat','MB_bloat','Table_MB'], tablefmt='fancy_grid'))
    results= []
# Index Information which gives Size, If its a primary key index or not, Number of scans an Index has helped
def indexinfo(conn,schname):
    sql = '''SELECT
        t.schemaname,
            t.tablename,
            indexname,
            pg_size_pretty(pg_relation_size(quote_ident(t.tablename)::text)) AS table_size,
            pg_size_pretty(pg_relation_size(quote_ident(indexrelname)::text)) AS index_size,
            CASE WHEN indisunique THEN 'Y'
            ELSE 'N'
            END AS UNIQUE,
            idx_scan AS number_of_scans
        FROM pg_tables t
        LEFT OUTER JOIN pg_class c ON t.tablename=c.relname
        LEFT OUTER JOIN
            ( SELECT psai.schemaname,c.relname AS ctablename, ipg.relname AS indexname, idx_scan, indexrelname, indisunique FROM pg_index x
                JOIN pg_class c ON c.oid = x.indrelid
                JOIN pg_class ipg ON ipg.oid = x.indexrelid
                JOIN pg_stat_all_indexes psai ON x.indexrelid = psai.indexrelid AND psai.schemaname = '{schname1}' )
            AS foo
            ON t.tablename = foo.ctablename
        WHERE t.schemaname='{schname1}'
        ORDER BY 1,2,5'''.format(schname1=schname)
    searchsql='set search_path to {schname1}'.format(schname1=schname)
    query(conn,searchsql)
    results = query(conn,sql)
    print(colored ("\nIndex Info of \"" +schname +"\" schema is given below:","green"))
    print(tabulate(results,headers=['SchemaName','TableName','IndexName','table_size','index_size','indisunique','NumberOfIdxScans'], tablefmt='fancy_grid'))
    results = []
def tableinfo(conn,schname):
    sql ='''select table_schema,
            table_name,pg_size_pretty(pg_table_size('"'||table_name||'"')) AS table_size,
            pg_size_pretty(pg_indexes_size('"'||table_name||'"')) AS indexes_size,
            pg_size_pretty(pg_total_relation_size('"'||table_name||'"')) AS total_size
            FROM information_schema.tables
            where table_schema='{schname1}' order by table_name,table_size'''.format(schname1=schname)
    searchsql='set search_path to {schname1}'.format(schname1=schname)
    query(conn,searchsql)
    results = query(conn,sql)
    print(colored ("\nTable Info of \"" +schname +"\" schema is given below :","green"))
    print(tabulate(results,headers=['SchemaName','TableName','TableSize','IndexSize','TotalSize'], tablefmt='fancy_grid'))
    results=[]
# Defines the amount of times queries have accessed the cache & the storage for its fetches
def disk_cache_hits(conn):
    sql ='''with
        all_tables as
        (
        SELECT  *
        FROM    (
            SELECT  'all'::text as table_name,
                sum( (coalesce(heap_blks_read,0) + coalesce(idx_blks_read,0) + coalesce(toast_blks_read,0) + coalesce(tidx_blks_read,0)) )  as from_disk,
                sum( (coalesce(heap_blks_hit,0)  + coalesce(idx_blks_hit,0)  + coalesce(toast_blks_hit,0)  + coalesce(tidx_blks_hit,0))  ) as from_cache
                FROM  pg_statio_user_tables
             ) a
                WHERE   (from_disk + from_cache) > 0 -- discard tables without hits
            ),
       tables as
        (
        SELECT  *
        FROM    (
                SELECT  relname as table_name,
                ( (coalesce(heap_blks_read,0) + coalesce(idx_blks_read,0) + coalesce(toast_blks_read,0) + coalesce(tidx_blks_read,0)) ) as from_disk,
                ( (coalesce(heap_blks_hit,0)  + coalesce(idx_blks_hit,0)  + coalesce(toast_blks_hit,0)  + coalesce(tidx_blks_hit,0))  ) as from_cache
            FROM    pg_statio_user_tables --> change to pg_statio_USER_tables if you want to check only user tables (excluding postgres's own tables)
            ) a
        WHERE   (from_disk + from_cache) > 0 -- discard tables without hits
        )
        SELECT  table_name as "table name",
            from_disk as "disk hits",
            from_cache as "cache_hits",
            round((from_disk::numeric / (from_disk + from_cache)::numeric)*100.0,2) as "% disk hits",
            round((from_cache::numeric / (from_disk + from_cache)::numeric)*100.0,2) as "% cache hits",
            (from_disk + from_cache) as "total hits"
        FROM    (SELECT * FROM all_tables UNION ALL SELECT * FROM tables) a
        ORDER   BY (case when table_name = 'all' then 0 else 1 end), from_disk desc '''
    results = query(conn,sql)
    print(colored ("\nThe Disk hits & Cache hits are details below :","green"))
    print(tabulate(results,headers=['TableName','DiskHits','CacheHits','% DiskHits','% CacheHits','TotalHits'],tablefmt='fancy_grid'))
# Extract Analyse Info of each table
def analyseinfo(conn,schname):
    sql='''select schemaname,relname,last_analyze,last_autoanalyze,analyze_count,autoanalyze_count
           from pg_stat_all_tables
           where schemaname=\'{schname1}\' '''.format(schname1=schname)
    results = query(conn,sql)
    print(colored ("\nAnalyse Information of Tables in \"" +schname +"\" schema is given below:","green"))
    print (tabulate(results,headers=['SchemaName','TableName','LastAnalyze','LastAutoanalyze','AnalyzeCount','AutoAnalyzeCount'],tablefmt='fancy_grid'))
# Get the checkpoint info since that last restart
def checkpointinfo(conn):
    sql='''select checkpoints_timed, checkpoints_req,
       total_checkpoints,seconds_since_start / total_checkpoints / 60 AS minutes_between_checkpoints
       FROM
           (SELECT checkpoints_timed,checkpoints_req,
       EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time())) AS seconds_since_start,
       ( checkpoints_timed + checkpoints_req ) AS total_checkpoints FROM pg_stat_bgwriter) AS sub'''
    results = query(conn,sql)
    print(colored ("\nCheckpoint Info  (Since last DB restart)   : (Hint: ReqCheckpoints < TimedCheckpoints = Ideal)","green"))
    print (tabulate(results,headers=['TimedCheckpoints','ReqCheckpoints','TotalCheckpoints','MinutesBetweenCheckpoints'],tablefmt='fancy_grid'))
# Find the commit Ratio
def commitratio(conn):
    sql = '''SELECT 100 * xact_commit / (xact_commit + xact_rollback) as commit_ratio
             FROM pg_stat_database
             WHERE (xact_commit + xact_rollback) > 0 and datname=current_database()'''
    results = query(conn,sql)
    print ('Commit Ratio Percentage is       : %s %%' %results[0])
# Count of tables in a Schema
def tablecount(conn,schname):
    sql ="select count(*) from pg_tables where schemaname='{schname1}'".format(schname1=schname)
    results = query(conn,sql)
    print ("Count of tables in \"" +schname +"\" schema  are"+" "*9 +": %s " %results[0])
# Statistics of Top 10 SQL statements executed
def querystats(conn):
    statchecksql="select * from pg_available_extensions where name='pg_stat_statements'"
    results1 = query(conn,statchecksql)
    if not results1 or None in results1[0]:
        print(colored ("\n The Extension pg_stat_statements does not exist, Hence Unable to provide SQL statement stats","green"))
    else:
        setsearchpath= query(conn,"set search_path to public")
        sql='''SELECT substring(query, 1, 100) AS short_query,
               round(total_time::numeric, 2) AS total_time, calls,
               round(mean_time::numeric, 2) AS mean,
               round((100 * total_time / sum(total_time::numeric) OVER ())::numeric, 2) AS percentage_cpu,
               100.0 * shared_blks_hit /nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
               FROM pg_stat_statements ORDER BY percentage_cpu DESC LIMIT 10 '''
        results = query(conn,sql)
        print(colored ("\nStatistics of Top 10 SQL statements executed","green"))
        print (tabulate (results,headers=['Query','TotalTime','No.OfCalls','MeanTime','PercentageCpu','HitPercent'],tablefmt='fancy_grid'))
# Gives a indicator incase a table requires an Index(Based on the seqscan and Idxscan of the tables)
def missingidxcheck(conn,schname):
    sql=''' SELECT relname as TableName,
            seq_scan - idx_scan AS too_much_seq,
            CASE
                WHEN
                     seq_scan - coalesce(idx_scan, 0) > 0
                THEN
                     'Missing Index?'
           ELSE
                'OK'
           END,
           seq_scan, idx_scan
           FROM pg_stat_all_tables
           WHERE schemaname = '{schname1}'
           ORDER BY
           too_much_seq DESC'''.format(schname1=schname)
    results = query(conn,sql)
    if not results or None in results[0]:
        print(colored ("\nProbability for having an index for the tables are : None","green"))
    else:
        print(colored ("\nStats to show if a table could be missing an Index(Just Indicators):","green"))
        print (tabulate (results,headers=['TableName','DiffSeqscanNdIdxscan','Remark','seq_scan','idx_scan'],tablefmt='fancy_grid'))
    results= []
    # except:
    #     sql2= '''SELECT relname, seq_scan-idx_scan AS too_much_seq, CASE
    #         WHEN seq_scan-idx_scan>0 THEN 'Missing Index?' ELSE 'OK' END,
    #          pg_relation_size(relname::regclass) AS rel_size, seq_scan,
    #          idx_scan FROM pg_stat_all_tables WHERE schemaname='public'
    #          AND pg_relation_size(relname::regclass)>80000
    #          ORDER BY too_much_seq DESC'''.format(schname1=schname)
    #     results = query(conn,sql2)
    #     if not results or None in results[0]:
    #         print(colored ("\nProbability for having an index for the tables are : None","green"))
    #     else:
    #         print(colored ("\nStats to show if a table could be missing an Index(Just Indicators):","green"))
    #         print (tabulate (results,headers=['TableName','DiffSeqscanNdIdxscan','Remark','rel_size','seq_scan','idx_scan'],tablefmt='fancy_grid'))
    #     results= []
# The DB read and write stats percentage
def dbreadwritestats(conn):
    sql=''' SELECT stats_reset,
            round((sum(tup_fetched) / nullif(sum(tup_fetched+tup_inserted+tup_updated+tup_deleted),0))*100 ,2) as SELECT,
            round((sum(tup_inserted) / nullif(sum(tup_fetched+tup_inserted+tup_updated+tup_deleted),0))*100 ,2) as INSERT,
            round((sum(tup_updated) / nullif(sum(tup_fetched+tup_inserted+tup_updated+tup_deleted),0))*100 ,2) as UPDATE,
            round((sum(tup_deleted) / nullif(sum(tup_fetched+tup_inserted+tup_updated+tup_deleted),0))*100 ,2) as DELETE
            from pg_stat_database
            where datname=current_database() group by stats_reset'''
    results= query(conn,sql)
    print(colored ("\n Database Read/Write stats % given below","green"))
    print (tabulate (results,headers=['Last Stats Reset Time','SELECT %','INSERT %','UPDATE %','DELETE %'],tablefmt='fancy_grid'))
# Find Wraparound Info
def wraparoundinfo(conn):
    sql='''WITH max_age AS (
    SELECT 2000000000 as max_old_xid
        , setting AS autovacuum_freeze_max_age
        FROM pg_catalog.pg_settings
        WHERE name = 'autovacuum_freeze_max_age' )
, per_database_stats AS (
    SELECT datname
        , m.max_old_xid::int
        , m.autovacuum_freeze_max_age::int
        , age(d.datfrozenxid) AS oldest_current_xid
    FROM pg_catalog.pg_database d
    JOIN max_age m ON (true)
    WHERE d.datallowconn )
SELECT max(oldest_current_xid) AS oldest_current_xid
    , max(ROUND(100*(oldest_current_xid/max_old_xid::float))) AS percent_towards_wraparound
    , max(ROUND(100*(oldest_current_xid/autovacuum_freeze_max_age::float))) AS percent_towards_emergency_autovac
FROM per_database_stats'''
    results=query(conn,sql)
    print(colored ("\n Wraparound Details","green"))
    print (tabulate(results,headers=['Oldest Current xid','% towards wraparound','% towards emergency autovacuum'],tablefmt='fancy_grid'))
# Class to redirect all the print statements into a file(If "-f" is included in the arguments list) and also print the same on STDOUT
class MyWriter:
    def __init__(self, stdout, filename):
        self.stdout = stdout
        self.logfile = file(filename, 'a')
    def write(self, text):
        self.stdout.write(text)
        self.logfile.write(text)
    def close(self):
        self.stdout.close()
        self.logfile.close()
#main() function of the program
if __name__ == "__main__":
    #writer = MyWriter(sys.stdout, 'log.txt')
    #sys.stdout = writer
    conn = create_conn()
    cur = conn.cursor()
    if args.file :
       writer = MyWriter(sys.stdout, args.file)
       sys.stdout = writer
    print ("Date of Review:"),
    print (now.strftime("%Y-%m-%d %H:%M"))
    print(colored ("\n" + '*' * 25 +"  Instance Information below  " + '*' * 25, 'magenta',attrs=['bold']))
    hostparse()
    dbversion(conn)
    dbNameSize(conn)
    dbUptime(conn)
    dbreplicacheck(conn)
    print(colored ("\n" + '*' * 25 +"  DB configured Parameters " + '*' * 28,'magenta',attrs=['bold']))
    maxconnection(conn)
    sharedbuff(conn)
    workmem(conn)
    AutovacuumScaleFactor(conn)
    AutovacuumAnalyzeScaleFactor(conn)
    maintenanceworkmem(conn)
    effcachesize(conn)
    autovacuumsetting(conn)
    print(colored ("\n" + '*' * 25 +"  DB working Statistics " + '*' * 29,'magenta',attrs=['bold']))
    invalidIdx(conn)
    index_hitratio(conn)
    table_hitratio(conn)
    commitratio(conn)
    checkpointinfo(conn)
    wraparoundinfo(conn)
    dbreadwritestats(conn)
    SchemaName=dbschema(conn)
    if not SchemaName or None in SchemaName[0]:     # If there are no user defined scehmas & the tables are created in the public schema
        SchemaName = "public"
        print ("Only the Default Postgres Schema exists"+ ' ' *10 + ": Public")
        tablecount(conn,SchemaName)
        tableinfo(conn,SchemaName)
        indexinfo(conn,SchemaName)
        missingidxcheck(conn,SchemaName)
        analyseinfo(conn,SchemaName)
    else:
        for result_row in SchemaName:               # While there are user defined schema
            lenght = len(result_row)
            for i in range(lenght):
                tablecount(conn,result_row[i])
                tableinfo(conn,result_row[i])
                indexinfo(conn,result_row[i])
                missingidxcheck(conn,result_row[i])
                analyseinfo(conn,result_row[i])
    tableautovacuumconfig(conn)
    bloatinfo(conn)
    vacuumdetails(conn)
    dbtbwoprimarykey(conn)
    unusedidx(conn)
    tempfile()
    disk_cache_hits(conn)
    querystats(conn)
    close_conn(conn)
    print("Connection Closed!!")
