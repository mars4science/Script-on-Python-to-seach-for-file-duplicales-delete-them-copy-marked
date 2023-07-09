#!/usr/bin/python3

__version__ = "5.5, 2023 July 09"
# Python 3.8, Linux Mint 20.2/21 tested
# Only some earlier versions IIRC were run on Windows, it might still work or require minor changes to paths (/ vs \).

# Short summary (See README / --help output for details):
# Read files stats + checksum to database
# Identifies duplicate files (TO DO for whole folders)
# Sync folders using stored files stats
# Other commands, see --help, some details might be in changelog
# SQL code is not safe against injections, folders names are not expected to contain double quotation symbols

# TO DO:
# 0. code to identify duplicate folders as many software folders contain some of same files, delete only if whole folder matches - DONE ver 5.3
# 1. notUsefulEnd - change from list of items [] to any combination of items from list
# 2. implement deleting directly location against location - DONE ver 3.3
# 3. test on deleting from path which contained in database - duplicates inside one location, not one against the other
# 4. check on that:
# if updateDuringSearch:
# dbConnection.commit() needed if search for duplicates is made with single set of files, not one against the other; 
# changes results of outer select for FOR and slower than commit at the end; 
# may change outer query qty returned as working due to SQLite functioning, so be careful when searcing single set of data, not one against the other

from files_functions import uprint, end, make_db_table, delete_db_table, add_index_db_table_sha256, add_index_db_table_filepath 
import os
import sys
import sqlite3
import time
from time import gmtime, strftime, localtime
from datetime import datetime
from pathlib import Path
import time
import codecs
import hashlib
import platform
import shutil

import signal
def signal_handler(sig,frame):
    print("Seems like Ctrl-C pressed, commiting changes to db")  
    dbConnection.commit()
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

import argparse
from argparse import RawTextHelpFormatter
#from argparse import ArgumentParser

parser = argparse.ArgumentParser(description='Process file structures, deleting duplicates renaming retained files is useful if additional info is not contained in extention - part of file name after last . symbol; paths better be passed as absolute', formatter_class=RawTextHelpFormatter)
parser.add_argument('command', choices=['read','add', 'search','totals', 'delete', 'deletemarked', 'compareonly', 'change', 'copy', 'deletesame', 'makedirs', 'sync', 'sync2', 'deletefolders'], help='command name: \nread - adds files in --filespath to database --db (modification date, size, sha256sum, path, name, --disk), \nadds - same as read but adds only those that are not already in --db (checks for same --disk AND path that includes name), \nsearch - outputs found files and info on them, \ntotals - outputs totals, \ndelete - deletes files in path (--files_d) against database (--db) or other path (--files) by file sha256, name, size and modification time and only if file is found on each of all disks (--disks can be several times), also --notchecktime --mne --mnb --nmn --rename (optional), \ndeletemarked - deleting (and renaming) what is marked already in database (by action field set to "todelete" in files_todelete table; if need to redo deletion for another disk, please run "change" to semi-manually change action field) and --files_d is used to add to path stored in database at beginning and --disk is used to delete marked for that disk only as a safeguard, delete from temp table, rename what is in main table, \ncompareonly - run only matching procedure for two tables in database which should be filled in already, \ncopy - copy files from one location (--files) to other (--files_c) for those files where action field in database (--db) is set to "tocopy" for specific --disk, \ndeletesame - delete duplicates in same location (--files_d) by filesize, sha256; also by name (exact or not, partial matching option same effect as do not match) and timestamp (exact ot not), \nmakedirs - make directories in path of files_c from filesdata entries in database, \nsync - add files absent on one disk/location (optional --pattern to select only part of filepaths, e.g. /folder1/%%) to another disk/location and update the db, need disk, disk_c - to locate files in db, files, files_c - paths to roots of locations to copy from and copy to (paths from db are appended to them), \nsync2 - same as sync but do both ways, from files to files_c then from files_c to files, \ndeletefolders - delete top level folder(s) recursively in provided --files_d path if complete matched folders contents are found in database (--db) or other path (--files) by file sha256, name, size and modification time and only if file is found on each of all disks (--disks can be several times), also --notchecktime (optional)')

parser.add_argument('--version', action = 'version', version='%(prog)s version '+ __version__)
parser.add_argument('--verbose', action='store_true', dest='verbose', help='output additional info, default: no output')
parser.set_defaults(verbose=False)
parser.add_argument('--db', default='./temp.db', help='full path to database location, default = temp.db in current folder')
parser.add_argument('--files', help='full path to the only/main file structure')
parser.add_argument('--files_d', help='full path to other file structure - where objects need to be deleted')
parser.add_argument('--files_c', help='full path to other file structure - whereto objects need to be copied for copy/or folders be created for makedirs')
parser.add_argument('--disk', help='disk name tag of file structure info - for add, read, totals, search, sync, sync2')
parser.add_argument('--disk_c', help='disk name tag to copy files to, used by sync, sync2 commands')
parser.add_argument('--disks', action='append', help='disk name tags when searched for candidates for deletion against database, if present, file should be present on all disks in main table to be considered a candidate, if omitted, should be present in main table as a whole. Should be one name per argument, several arguments possible, NOT several in one argument separated by comma')
parser.add_argument('--pattern', help='filename expression for search/filepath expression for sync (optional for sync), %% - percentage sign symbol can be used as any number of any symbols, ignore case, _ symbol means any AFAIK, for exact search add --exact parameter') # symbol % in help string gives argparse error on parse_args() line
parser.add_argument('--action', help='action text to search, usefull after processing, e.g. set to "deleted" if deleted') 

parser.add_argument('--notchecktime', action='store_false', dest='checkTime', help='when looking for duplicates, do not check that timestamp (modification time) is the same, default: check time')
parser.add_argument('--notmatchtime', action='store_false', dest='checkTime', help='when looking for duplicates, do not check that timestamp (modification time) is the same, default = check time, same effect as notchecktime')
parser.set_defaults(checkTime=True)
parser.add_argument('--mne', dest='filenamematchway', action='store_const', const='matchfilenamesexactly', default = 'matchfilenamesexactly', help='when looking for duplicates, to match file names exactly, this is default')
parser.add_argument('--mnb', dest='filenamematchway', action='store_const', const='matchfilenambeginnings', help='when looking for duplicates, to match file names where one name begins with full other name (w/out extention)')
parser.add_argument('--nmn', dest='filenamematchway', action='store_const', const='notmatchfilenames', help='when looking for duplicates, do not check file names, by other file data only')
parser.add_argument('--simulateonly', action='store_true', help='do not actually delete files on disk, action is db is set to "deleted" still') # defaults to opposite of action if action='store_true' or 'store_false'
parser.add_argument('--tmp', action='store_true', help='for search and totals - use tmp table in db, default: main table')
parser.add_argument('--exact', action='store_true', help='for search - use exact filename match, default: LIKE clause for SQL')
parser.add_argument('--rename', action='store_true', dest='rename', default= False, help='rename retained files with additional potentially useful info from deleted files names, default - analyse names and store in db, do not rename on disk')
parser.add_argument('--qty', default = 1000000, type=int, help='number of files expected to be processed, default = 1 000 000')
parser.add_argument('--parts', default = 100, type=int, help='how many times to report intermidiary process status, default = 100')

args = parser.parse_args()

MainAction = args.command

full_path = args.files
full_path_d = args.files_d
full_path_c = args.files_c
dblocation = args.db
diskname = args.disk
diskname_c = args.disk_c
disks = args.disks
FileNameEx = args.pattern
filestoprocess = args.qty
qtyofparts = args.parts
filenamematchway = args.filenamematchway
simulateonly = args.simulateonly
checkTime = args.checkTime
rename_files = args.rename
verbose=args.verbose
#print (rename_files)
#exit()


use_temp_table = args.tmp
exact_search = args.exact
action = args.action

if disks == None:
    disks = [None]

if MainAction in ['delete', 'deletesame', 'deletefolders']:
    diskname = 'temp'

#qw = 'err'
#disks += [qw]
#print (len(disks))
#print (disks)
#end()
togo = True

# when path is copied from Nemo (ctrl-l) it is w/out trailing /, when typed in bash using tab for completion it is with /
# code stores part of path excluding paths starting locations in parameters, to ensure consistenty store with leading /
# remove trailing / if was in parameters
if full_path != None and full_path[-1:] == "/":
    full_path = full_path[:-1]
if full_path_d != None and full_path_d[-1:] == "/":
    full_path_d = full_path_d[:-1]
if full_path_c != None and full_path_c[-1:] == "/":
    full_path_c = full_path_c[:-1]

if full_path != None and full_path == full_path_d:
    print ('- paths to files same: --files and --files_d, terminating.\n To delete in same location: deletesame command with only --files_d.')
    togo = False

if MainAction in ['add', 'search', 'totals', 'change', 'copy', 'deletemarked', 'makedirs', 'sync', 'sync2'] and dblocation == './temp.db':
    print ('- path to database ("--db") is required for this command, if you want to use ./temp.db, please give another path version to it. e.g. full path')
    togo = False

if MainAction in ['add', 'search', 'totals', 'change', 'copy', 'deletemarked', 'makedirs', 'sync', 'sync2'] and not Path(dblocation).exists():
    print ("- path --db '%s' not found, typo?" % dblocation)
    togo = False

if MainAction in ['search'] and FileNameEx == None:
    print ('- search pattern ("--pattern") is required for this command')
    togo = False

if MainAction in ['add', 'read', 'clean', 'copy', 'sync', 'sync2'] and full_path == None:
    print ('- path to file structure ("--files") is required for this command')
    togo = False

if MainAction in ['copy', 'makedirs', 'sync', 'sync2'] and full_path_c == None:
    print ('- path to second file structure (where to copy - "--files_c") is required for this command')
    togo = False

if MainAction in ['read', 'copy', 'deletemarked', 'makedirs', 'add', 'sync', 'sync2'] and diskname == None:
    print ('- diskname ("--disk") is required for this command')
    togo = False
 
if MainAction in ['delete', 'deletemarked', 'deletesame'] and full_path_d == None:
    print ('- path to file structure where deletion is expected (--files_d) is required for this command')
    togo = False

if MainAction in ['delete', 'deletefolders']:
    if not ((full_path == None) ^ (dblocation == './temp.db')): # ^ is logical xor here
        print ('- either path to file structure (--files) or database (--db) to check againt is required for this command (not both though)')
        togo = False
    if dblocation != './temp.db' and not Path(dblocation).exists():
        print ("- path --db '%s' not found, typo?" % dblocation)
        togo = False

if MainAction in ['sync', 'sync2'] and diskname_c == None:
    print ('- diskname to copy to ("--disk_c") is required for this command')
    togo = False

if full_path != None and not Path(full_path).exists():
    uprint(" --full_path '%s' not found, typo?" % full_path)
    togo = False

if full_path_c != None and not Path(full_path_c).exists():
    uprint(" --full_path_c '%s' not found, typo?" % full_path_c)
    togo = False

if full_path_d != None and not Path(full_path_d).exists():
    uprint(" --full_path_d '%s' not found, typo?" % full_path_d)
    togo = False

if togo == False:
    print ('Next run please take into account remarks listed above') 
    exit (1)

tablename_main = 'filesdata'
tablename_temp = 'filesdata_todelete'
    
# https://stackoverflow.com/questions/110362/how-can-i-find-the-current-os-in-python
systemType = platform.system()
if dblocation == './temp.db':
    if systemType == 'Windows':
        dblocation = 'temp.db' # r'a:/listfiles.db'.replace ('\\','/')
    elif systemType == 'Linux':
        pass # dblocation = r'/home/user/listfiles.db'
    else:
        print ('Neither Windows nor Linux, not sure where to put database in file system, exiting...')
        end ()

# connect to database and then allow to use columns names
dbConnection = sqlite3.connect(dblocation, 1) # wait not default but 1 second is DB is locked
dbConnection.row_factory = sqlite3.Row

# to monitor time efficiency
print ()
print ('Start:')
#print (strftime("%Y-%m-%d %H:%M:%S", localtime())) # does not support franctions of seconds
print (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
print ()
startTime = datetime.today()

if MainAction in ['add', 'copy', 'deletemarked', 'makedirs', 'add', 'sync', 'sync2']:
    row = dbConnection.execute('SELECT * FROM ' + tablename_main + ' WHERE disk = "' + diskname + '" limit 1').fetchone()
    if row == None:
        uprint("--disk '%s' not found in db '%s', terminating...\n" % (diskname,dblocation))
        if MainAction in ['add']:
            uprint("maybe use read command?")
        end(startTime, dbConnection)

if MainAction in ['sync', 'sync2']:
    row = dbConnection.execute('SELECT * FROM ' + tablename_main + ' WHERE disk = "' + diskname_c + '" limit 1').fetchone()
    if row == None:
        uprint("--disk '%s' not found in db '%s', terminating...\n" % (diskname_c,dblocation))
        end(startTime, dbConnection)

if MainAction in ['delete', 'deletefolders'] and dblocation != './temp.db' and disks != [None]:
    for disk in disks:
        row = dbConnection.execute('SELECT * FROM ' + tablename_main + ' WHERE disk = "' + disk + '" limit 1').fetchone()
        if row == None:
            uprint("--disk '%s' not found in db '%s', terminating...\n" % (disk,dblocation))
            end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------#
# to copy files set to be copied (field 'action' is 'tocopy') - see help for detailes

def copymarked(disk_name, source_path, dest_path):

    uprint("----- copying files marked 'tocopy' in 'action' field for disk '" + disk_name + "' from '" + source_path + "' to '" + dest_path  + "' -----\n" )

    notcopiedasnotfound = 0
    notcopiedasfailed = 0
    notcopiedaswerethere = 0
    copied = 0

    # c = dbConnection.execute('select id, filepath from "' + tablename_main + '" where action="tocopy" AND disk = "' + disk_name + '"')
    # c.rowcount gave -1, so need another way
    c = dbConnection.execute('select count(*) as qty from "' + tablename_main + '" where action="tocopy" AND disk = "' + disk_name + '"')
    row = c.fetchone()
    filestoprocess = row['qty']

    processed = 0
    partprocessed = 0
    print ("Files to process: ", filestoprocess)
    print ()

    c = dbConnection.execute('select id, filepath from "' + tablename_main + '" where action="tocopy" AND disk = "' + disk_name + '"')
    for row_c in c:
        # uprint(row_c['filepath'])
        srcfile = source_path + row_c['filepath']
        try_success = -10
        try_number = 3 # number of tries in case of IO error
        try_to = try_number

        while try_to>0:
            try:
                source_found=Path(srcfile).is_symlink() or Path(srcfile).exists()
                try_to=try_success
            except Exception as e:
                uprint ("(Un?)expected error when check existence of source file: " + str(e))
                uprint ("Wating several seconds and trying again...")
                time.sleep(10)
                try_to -= 1
        if try_to != try_success:
            uprint ('Seems that read of source info failed repeatedly, terminating.')
            break # for loop
        else:
            if not source_found: #.is_file()
                uprint ('not found: ',srcfile)
                notcopiedasnotfound += 1
                d = dbConnection.execute('UPDATE "' + tablename_main + '" SET action = "not copied as not found" WHERE id = ' + str(row_c['id']))
            else:
                dstfile = dest_path + row_c['filepath']
                try_success = -10
                try_number = 3 # number of tries in case of IO error
                try_to = try_number
                while try_to>0:
                    try:
                        destination_found=Path(dstfile).is_symlink() or Path(dstfile).exists()
                        try_to=try_success
                    except Exception as e:
                        uprint ("(Un?)expected error when check existence of destination file: " + str(e))
                        uprint ("Wating several seconds and trying again...")
                        time.sleep(10)
                        try_to -= 1
                if try_to != try_success:
                    uprint ('Seems that read of destination info failed repeatedly, terminating.')
                    break # for loop
                else:

                    if destination_found:
                        uprint ('already there: ',dstfile)
                        notcopiedaswerethere += 1
                        d = dbConnection.execute('UPDATE "' + tablename_main + '" SET action = "not copied as was already there" WHERE id = ' + str(row_c['id']))
                    else:
                        dstdir =  os.path.dirname(dstfile)
                        try:
                            os.makedirs(dstdir) # create all directories, might give an error if it already exists (catch here and pass)
                        except Exception as e:
                            pass
                        try:
                            shutil.copy2(srcfile, dstdir, follow_symlinks=False)
                            copied +=1
                            d = dbConnection.execute('UPDATE "' + tablename_main + '" SET action = "copied" WHERE id = ' + str(row_c['id']))
                        except Exception as e:
                            notcopiedasfailed +=1
                            d = dbConnection.execute('UPDATE "' + tablename_main + '" SET action = "not copied as copy failed" WHERE id = ' + str(row_c['id']))

        processed += 1
        if (processed / filestoprocess) > (partprocessed / qtyofparts):
            partprocessed += 1
            print (strftime("%Y-%m-%d %H:%M:%S", localtime()))
            print ('part ', partprocessed, ' of ', qtyofparts, ' current item number ', processed, ' copied: ', copied)
            print ()

    dbConnection.commit()

    print ('Number of files that have been copied         : {:,.0f}'.format(copied).replace(',', ' '))
    print ('Number of files that have been not been found : {:,.0f}'.format(notcopiedasnotfound).replace(',', ' '))
    print ('Number of files that have been there already  : {:,.0f}'.format(notcopiedaswerethere).replace(',', ' '))
    print ('Number of files that had errors on copy       : {:,.0f}'.format(notcopiedasfailed).replace(',', ' '))
    print ()


if MainAction == 'copy':
    copymarked(diskname,full_path,full_path_c)
    end(startTime, dbConnection)


# ----------------------------------------------------------------------------------------------------------------------------------------------------#
# to make folders (field 'action' is 'makedirs') - see help for detailes

if MainAction == 'makedirs':

    c = dbConnection.execute('select id, filepath from "' + tablename_main + '" where disk = ?', (diskname,))
    for row_c in c:
        dstfile = full_path_c + row_c['filepath']
        dstdir =  os.path.dirname(dstfile)
        try:
            os.makedirs(dstdir) # create all directories, raise an error if it already exists
        except Exception as e:
            pass

    end(startTime, dbConnection)


# ----------------------------------------------------------------------------------------------------------------------------------------------------#
# to change info in database (expected to be edited before run)- maybe usefull if SQL tool is not used

if MainAction == 'change':
    #c = dbConnection.execute('alter table filesdata add column %s TEXT' % 'runID') # 'alter table filesdata add column "%s" "TEXT"' % columnName

    #c = dbConnection.execute('UPDATE filesdata SET action = NULL WHERE action <> "123deleted"') # AND (runID NOT LIKE "music%" OR runID IS NULL)
    #c = dbConnection.execute('UPDATE filesdata SET disk = "4tb-18" WHERE disk IS NULL') #  AND filepath LIKE "_all\Music\%"
    #c = dbConnection.execute('DELETE FROM filesdata WHERE disk = "4tb-57" AND filepath LIKE "_all\Music\%"')
    #c = dbConnection.execute('INSERT INTO filesdata (filename, filepath, disk, filetime, filesize, action) SELECT filename, substr (filepath, 4), disk, CAST(filetime AS INTEGER), filesize, action FROM newfiles WHERE disk <>"1AS4t-57"')
    # to compact db
    # c = dbConnection.execute('VACUUM')

    # SQLite doesn't support JOINs in UPDATE statements, below does not work
    # c = dbConnection.execute('UPDATE newfiles nfu JOIN newfiles nfc ON  (nfu.filename = nfc.filename) SET action = "notdeleted" WHERE nfu.disk = "750g" ')
    c = dbConnection.execute('UPDATE filesdata_todelete SET action = "todelete" WHERE action == "deleted"')
    dbConnection.commit()
    print ('number of rows attected: ', c.rowcount)
    end(startTime, dbConnection)


# ----------------------------------------------------------------------------------------------------------------------------------------------------#

if MainAction == 'totals':

    d = dbConnection.execute('SELECT disk FROM ' + (tablename_temp if use_temp_table else tablename_main) + ' GROUP BY disk') 

    for row_d in d:

        c = dbConnection.execute('SELECT disk, filesize, filename, filenamenew, filepath, filetime, action, runID, sha256, sha256_start, sha256_end FROM ' + (tablename_temp if use_temp_table else tablename_main) + ('' if (row_d['disk'] is None) else ' WHERE disk = "' + row_d['disk'] + '"')) 

        hasher = hashlib.sha256() #md5
        hasher_start = hashlib.sha256()
        hasher_end = hashlib.sha256()

        filesfound = 0
        files_size = 0

        for row in c:
            hasher.update(row['sha256'].encode('utf-8'))
            filesfound += 1
            files_size += row['filesize']

        print('-----------------------------------------------------------------------------')
        print('Disk       : ' + row_d['disk'])
        print('Files qty  : {:,.0f}'.format(filesfound).replace (',',' '))
        print('Total bytes: {:,.0f}'.format(files_size).replace (',',' '))
        print('Hash       : ' + hasher.hexdigest())
        print()

        c = dbConnection.execute('SELECT disk, action, sum (filesize) as Total, count (filesize) as Qty FROM "' + tablename_main + '" WHERE (action <> "SOMETHINGdeleted" OR action IS NULL)' + ('' if (row_d['disk'] is None) else ' AND disk = "' + row_d['disk'] + '"') + ' GROUP BY action')

        # https://docs.python.org/2/library/string.html#format-specification-mini-language
        for row in c:
            uprint ('Action     : ' + str (row['action']))
            uprint ('Total qty  : {:,.0f}'.format(row['Qty']).replace (',',' ')) 
            uprint ('Total bytes: {:,.0f}'.format(row['Total']).replace (',',' '))
            print ()

    print('-----------------------------------------------------------------------------')
    end(startTime, dbConnection)


# ----------------------------------------------------------------------------------------------------------------------------------------------------#

if MainAction == 'search':

    # c = dbConnection.execute('SELECT disk, filesize, filename, filenamenew, filepath, filetime, action, runID FROM "' + tablename_main + '" WHERE disk = "Test" AND filepath LIKE "%alex%" AND filename LIKE "%a%" ORDER BY filepath ASC') 
    c = dbConnection.execute('SELECT disk, filesize, filename, filenamenew, filepath, filetime, action, runID, sha256, sha256_start, sha256_end FROM ' + (tablename_temp if use_temp_table else tablename_main) + ' WHERE filename ' + ('=' if exact_search else 'LIKE') + ' "' + FileNameEx +  '"' + ('' if (diskname is None) else ' AND disk = "' + diskname + '"') + ('' if (action is None) else ' AND action = "' + action + '"') + '   ORDER BY filepath ASC') # WHERE sha256 IS NOT NULL AND filesize <> 71 

    #  filenamenew IS NOT NULL AND  OR action IS NULL runid = "music 1" AND  AND (action = "todelete") FileNameEx

    filesfound = 0
    for row in c:
        uprint ('Disk         : ', row['disk'])
        uprint ('Filename     : ', row['filename'])
        uprint ('Filename new : ', row['filenamenew'])
        uprint ('Size         : ', '{:,.0f}'.format(row['filesize']).replace (',',' '))
        uprint ('Filetime     : ', '{:,.0f}'.format(row['filetime']).replace (',',' '))
        uprint ('Filepath     : ', row['filepath'])
        uprint ('action       : ', row['action'])
        uprint ('Sha256       : ', row['sha256'])
        uprint ('Sha256 start : ', row['sha256_start'])
        uprint ('Sha256   end : ', row['sha256_end'])
        uprint ('Run ID       : ', row['runID'])
        uprint ()
        filesfound += 1

    print ('Found: ', filesfound)
    end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------#
#
# compare entries in database tables (made from files in folders)

def comparefiles (tablepossibleduplicates, tablemain):

    # looks through files in database in table tablepossibleduplicates and searches for similar files in table tablemain - with same name (depends on filenamematchway), size, modification date (depends on checkTime) and same sha256
    # if 'same' file found script marks file from tablepossibleduplicates as 'todelete'
    # making for deletion is done only if file is found on each of all disks ([disks] variable)

    uprint("----- compare files marking found duplicates for deletion searching in table '" + tablepossibleduplicates + "' for possible duplicates in table '" + tablemain + "' -----\n" )

    # added to speed up search many times in case of large number of entries to matches against
    add_index_db_table_sha256 (dbConnection, tablemain)
    
    runID = 'pc 1'
    deletionFilter = '(action <> "deleted" AND action <> "todelete" OR action is NULL)'

    # if folders overlap, need to update database continuously else duplicates will be deleted both - NOT APPLICABLE HERE, HENCE FALSE
    # if True not tested after code was modified
    updateDuringSearch = False

    # choose location where to delete, if True then deleting in inner loop, otherwise in outer
    # when True not tested !!!
    deleteInFoundNotInFind = False

    checkSize = 1 # if 1 exact match by size, otherwise as fraction needed for match - usefull for mp3 as metadata- not used now

    # what deleted file name tail to ignore when renaming retained file (if deleted file is named longer - hence name may contain additional info)
    not_useful_ending = [' (1)',' (2)',' (3)', ' (copy)',' (another copy)']

    processed = 0
    partprocessed = 0
    recommended_for_deletion = 0
    recommendedNameChange = 0

    myQuery = 'SELECT id, disk, filename, filenamenew, filepath, filesize, filetime, sha256, sha256_start, sha256_end FROM ' + tablepossibleduplicates + ' WHERE ' + deletionFilter

    c_find = dbConnection.execute(myQuery)

    for rowToFind in c_find:
        found_qty = 0
        found_on_disks = []

        try:
            #uprint (rowToFind['filepath'], rowToFind['filesize'], rowToFind['filetime'], ' tofind')  
            for disk in disks:

                match_found = False

                c_found = dbConnection.execute('SELECT id, disk, filename, filenamenew, filepath, filesize, filetime FROM ' + tablemain + ' WHERE ' + deletionFilter + ' AND filesize = ? AND sha256 = ? AND sha256_start = ? AND sha256_end = ?' + ('' if disk == None else ' AND disk = "' + disk +'"') + (' AND ABS (filetime - {0}'.format(rowToFind['filetime']) +') <=1' if checkTime else ''),(rowToFind['filesize'],rowToFind['sha256'],rowToFind['sha256_start'],rowToFind['sha256_end'])) # ' AND filetime = ?' if checkTime else '?' - does no work, use <> 1 again

                for rowFound in c_found:

                    #uprint (rowFound['filepath'], rowFound['filesize'], rowFound['filetime'], ' found') 

                    new_in_db_file_name = rowToFind['filenamenew'] if         deleteInFoundNotInFind else rowFound['filenamenew']   
                    prev_file_name      = rowToFind['filename']    if         deleteInFoundNotInFind else rowFound['filename']            
                    new_file_name       = rowToFind['filename']    if not     deleteInFoundNotInFind else rowFound['filename']
                    rowToDelete         = rowToFind['id']          if not     deleteInFoundNotInFind else rowFound['id']
                    rowToUpdate         = rowToFind['id']          if         deleteInFoundNotInFind else rowFound['id']

                    if filenamematchway == 'matchfilenamesexactly':
                        if prev_file_name == new_file_name:
                            match_found = True # do_marking_for_deletion = True
                    else:
                        new_ext_location  = new_file_name.rfind ('.')
                        prev_ext_location = prev_file_name.rfind ('.')
                        if new_ext_location == -1: # '.' not found 
                            new_file_name_no_ext = new_file_name
                        else:
                            new_file_name_no_ext = new_file_name[0:new_ext_location] 
                        if prev_ext_location == -1: # '.' not found 
                            prev_file_name_no_ext = prev_file_name
                        else:
                            prev_file_name_no_ext = prev_file_name[0:prev_ext_location]

                        # rename is usefull only if new name is longer
                        if len (new_file_name_no_ext) > len (prev_file_name_no_ext) and new_file_name_no_ext[0:len(prev_file_name_no_ext)] == prev_file_name_no_ext:
                                match_found = True # do_marking_for_deletion = True

                                if ((new_in_db_file_name is None) or len(new_file_name) > len(new_in_db_file_name)) and new_file_name_no_ext[len(prev_file_name_no_ext):len(new_file_name_no_ext)] not in not_useful_ending:
                                    dbConnection.execute('UPDATE ' + (tablepossibleduplicates if deleteInFoundNotInFind else tablemain) + ' SET runID = ?, filenamenew = ? WHERE id = ?',(runID, new_file_name, rowToUpdate, ))
                                    dbConnection.execute('UPDATE ' + (tablepossibleduplicates if deleteInFoundNotInFind else tablemain) + ' SET runID = ?, action = "toupdate" WHERE id = ?',(runID, rowToUpdate, ))
                                    recommendedNameChange += 1
                        else:
                            if filenamematchway == 'notmatchfilenames':
                                match_found = True # do_marking_for_deletion = True
                            else:
                                if filenamematchway == 'matchfilenambeginnings' and len (new_file_name_no_ext) <= len (prev_file_name_no_ext) and prev_file_name_no_ext[0:len(new_file_name_no_ext)] == new_file_name_no_ext:
                                    match_found = True # do_marking_for_deletion = True

                    if match_found and not deleteInFoundNotInFind:
                        break # from inner FOR (for rowFound in c_found:) as there is no obvious point to search for new originals after one matching is found except maybe for renaming all, but that is not the goal of script at least for now                

                if match_found:
                    found_qty += 1
                    found_on_disks += [disk]

            # mark for deletion
            if found_qty == len (disks):
                dbConnection.execute('UPDATE ' + (tablepossibleduplicates if not deleteInFoundNotInFind else tablemain) + ' SET runID = ?, action = "todelete" WHERE id = ?',(runID, rowToDelete, ))
                recommended_for_deletion += 1
            
                if updateDuringSearch:
                    dbConnection.commit() # needed if search for duplicates is made with single set of files, not one against the other; changes results of outer select for FOR and slower than commit at the end; may change outer query qty returned as working due to SQLite functioning, so be careful when searcing single set of data, not one against the other
            elif found_qty > 0:
                uprint ("File: ", 'id: ', rowToFind['id'], 'filename: ', rowToFind['filename'], 'filesize: ', rowToFind['filesize'], 'filetime: ', rowToFind['filetime'], ' found only on disks: ', found_on_disks)
                
        except Exception as e:
            uprint ("(Un?)expected error: " + str(e))
            uprint ("Database error on:", 'id: ', rowToFind['id'], 'filename: ', rowToFind['filename'], 'filesize: ', rowToFind['filesize'], 'filetime: ', rowToFind['filetime'])

        processed += 1
        if (processed / filestoprocess) > (partprocessed / qtyofparts):
            partprocessed += 1
            print (strftime("%Y-%m-%d %H:%M:%S", localtime()))
            print ('part ', partprocessed, ' of ', qtyofparts, ' current item number ', processed, ' marked for deletion: ', recommended_for_deletion)
            print ()

    dbConnection.commit()
    print ('Files marked for deletion: ', "{:,.0f}".format(recommended_for_deletion).replace(",", " "), ' out of:', "{:,.0f}".format(processed).replace(",", " "))
    print ('Recommended for name change: ', "{:,.0f}".format(recommendedNameChange).replace(",", " "))
    print()

# ----------------------------------------------------------------------------------------------------------------------------------------------------#
#
# compare folders along with all their contents

def comparefolders (tablepossibleduplicates, tablemain):

    # looks through folders in database in table tablepossibleduplicates and searches for same folders (all files) in table tablemain - files with same name, size, modification date (depends on checkTime) and same sha256
    # match is searched for in all disks ([disks] variable)

    uprint("----- compare folders for future deletion taking top folders from table '" + tablepossibleduplicates + "' and checking against possible duplicates in table '" + tablemain + "' -----\n" )

    # added to speed up search many times in case of large number of entries to matches against
    add_index_db_table_sha256 (dbConnection, tablemain)

    runID=strftime("%Y-%m-%d %H:%M:%S", localtime())
    deletionFilter = '(action <> "deleted" AND action <> "todelete" OR action is NULL)'

    # if folders overlap, need to update database continuously else duplicates will be deleted both - NOT APPLICABLE HERE, HENCE FALSE
    # if True not tested after code was modified
    updateDuringSearch = False

    processed = 0
    recommended_for_deletion = 0

    # folder != "/" added to skip that SELECT result in case files are present on top level, not folders only
    myQuery = 'SELECT distinct substr(filepath,1,instr(substr(filepath,2),"/")+1) as folder FROM ' + tablepossibleduplicates + ' WHERE folder != "/" and ' + deletionFilter
    folder_tofind = dbConnection.execute(myQuery)

    for folderToFind in folder_tofind:
        found_qty = 0
        found_on_disks = []

        try:

            for disk in disks:

                match_found = False
                folder_found = dbConnection.execute('SELECT distinct disk,substr(filepath,1,instr(filepath,"' + folderToFind['folder'] + '")+length("' + folderToFind['folder'] + '")-1) as folder FROM ' + tablemain + ' WHERE ' + deletionFilter + ' AND filepath like ? ' + ('' if disk == None else ' AND disk = "' + disk +'"'),('%' + folderToFind['folder'] + '%',))

                for folderFound in folder_found: # one fully matched is enough

                    # compare number of files, if match, then need to only compare files in one to the other to ensure full two-way match
                    row = dbConnection.execute('SELECT count(id) as qty FROM ' + tablepossibleduplicates + ' WHERE ' + deletionFilter + ' AND filepath like ?',(folderToFind['folder'] + '%',)).fetchone()
                    qty_find = row['qty']
                    row = dbConnection.execute('SELECT count(id) as qty FROM ' + tablemain + ' WHERE ' + deletionFilter + ' AND filepath like ? AND disk = ?',(folderFound['folder'] + '%',folderFound['disk'])).fetchone()
                    qty_found = row['qty']
                    if qty_found != qty_find:
                        if verbose:
                            uprint ("Qty not matched:", folderToFind['folder'], qty_find, folderFound['folder'], qty_found)
                        continue # not matched, check next folderFound

                    file_find = dbConnection.execute('SELECT id, disk, filename, filenamenew, filepath, filesize, filetime, sha256, sha256_start, sha256_end FROM ' + tablepossibleduplicates + ' WHERE ' + deletionFilter + ' AND filepath like ?',(folderToFind['folder'] + '%',))

                    for fileToFind in file_find:

                        file_found = dbConnection.execute('SELECT id, disk, filename, filenamenew, filepath, filesize, filetime, sha256, sha256_start, sha256_end FROM ' + tablemain + ' WHERE ' + deletionFilter + ' AND filesize = ? AND sha256 = ? AND sha256_start = ? AND sha256_end = ? and filepath = ? AND disk = ?' + (' AND ABS (filetime - {0}'.format(fileToFind['filetime']) +') <=1' if checkTime else ''),(fileToFind['filesize'],fileToFind['sha256'],fileToFind['sha256_start'],fileToFind['sha256_end'], folderFound['folder'] + fileToFind['filepath'][len(folderToFind['folder']):], folderFound['disk'])).fetchone() # ' AND filetime = ?' if checkTime else '?' - does no work, use <> 1 again
                        # uprint (fileToFind['filepath'],fileToFind['sha256'],folderFound['disk'])
                        if file_found == None:
                            if verbose:
                                uprint ("NO matched file found for:", fileToFind['filepath'], fileToFind['sha256'], "in:", folderFound['folder'])
                            break

                    else: # only executed if innner loop did NOT break
                        if verbose:
                            uprint ("matched file found for:", fileToFind['filepath'],fileToFind['sha256'],"in:", folderFound['folder'])
                        match_found = True

                    if match_found:
                        break # from FOR as there is no obvious point to search for new originals after one matching is found

                if match_found:
                    found_qty += 1
                    found_on_disks += [disk]

            # mark for deletion
            if found_qty == len (disks):
                uprint (folderToFind['folder'][1:-1], ' matched')
                recommended_for_deletion += 1
                dbConnection.execute('UPDATE ' + tablepossibleduplicates + ' SET runID = ?, action = "todelete" WHERE filepath like ?',(runID, folderToFind['folder'] + '%'))
                if updateDuringSearch:
                    dbConnection.commit() # needed if search for duplicates is made with single set of files, not one against the other; changes results of outer select for FOR and slower than commit at the end; may change outer query qty returned as working due to SQLite functioning, so be careful when searcing single set of data, not one against the other

            elif found_qty > 0:
                uprint ("Folder: ", folderToFind['folder'][1:-1], ' found only on disks: ', found_on_disks)
            else:
                uprint ("Folder: ", folderToFind['folder'][1:-1], ' NOT matched')

        except Exception as e:
            uprint ("(Un?)expected error: " + str(e))
            uprint ("(?) Database error")

        processed += 1

    dbConnection.commit()
    print ('Folders matched for deletion: ', "{:,.0f}".format(recommended_for_deletion).replace(",", " "), ' out of:', "{:,.0f}".format(processed).replace(",", " "))
    print()

# ----------------------------------------------------------------------------------------------------------------------------------------------------#
#
# delete files marked for deletion
def deletefiles(targetpath, tablename):

    uprint("----- deleting files marked 'todelete' in 'action' field in table '" + tablename + "' for disk '" + diskname + "' from '" + targetpath  + "' -----\n" ) 

    processed = 0
    qtyofparts = 10
    partprocessed = 0
    filesdeleted = 0
    deleteErrors = 0
    filestoprocess = 0

    myQuery = 'SELECT id, filename, filepath , filesize, filetime FROM ' + tablename + ' WHERE action = "todelete" AND disk = "' + diskname + '"'

    c = dbConnection.execute(myQuery)

    filestoprocess = 0
    for row in c:
        filestoprocess += 1 

    c = dbConnection.execute(myQuery)

    for row in c:
        
        try:
            filepathTodelete = targetpath + row['filepath']

            if systemType == 'Linux':
                filepathTodelete = filepathTodelete.replace ('\\','/')
            else:
                filepathTodelete = filepathTodelete.replace ('/','\\')             

            if not simulateonly:
                os.remove(filepathTodelete)

            dbConnection.execute('UPDATE ' + tablename + ' SET action = "deleted" WHERE id = ?', (row['id'], ))
            filesdeleted += 1

        except FileNotFoundError as e:
            uprint ("FileNotFoundError: " + str(e))
            deleteErrors += 1

        processed += 1
        if (processed / filestoprocess) > (partprocessed / qtyofparts):
            partprocessed += 1
            print (strftime("%Y-%m-%d %H:%M:%S", localtime()))
            print ('part ', partprocessed, ' of ', qtyofparts, ' current item number ', processed, ' deleted: ', filesdeleted)
            print ()
 
    dbConnection.commit()
    print ('Files deleted: ', "{:,.0f}".format(filesdeleted).replace(",", " "), ' errors:', deleteErrors)
    print ()


# ----------------------------------------------------------------------------------------------------------------------------------------------------#

# rename files marked for renaming (does not overwrite file if same file with new name already exists)
def renamefiles(targetpath, tablename):

    processed = 0
    qtyofparts = 10
    partprocessed = 0
    filesrenamed = 0
    renameErrors = 0
    notrenamedduetoexistingfile = 0
    filestoprocess = 0

    myQuery = 'SELECT id, filename, filenameNew, filepath , filesize, filetime FROM ' + tablename + ' WHERE action = "toupdate"'

    c = dbConnection.execute(myQuery)

    filestoprocess = 0
    for row in c:
        filestoprocess += 1 

    c = dbConnection.execute(myQuery)

    for row in c:

        try:
            filepathnoname           = targetpath + row['filepath'][0:len(row['filepath'])-len(row['filename'])]
            filepathwithnameprevious = targetpath + row['filepath']
            filepathwithnamenew      = filepathnoname + row['filenameNew']

            if systemType == 'Linux':
                filepathwithnameprevious = filepathwithnameprevious.replace ('\\','/')
                filepathwithnamenew = filepathwithnamenew.replace ('\\','/')
            else:
                filepathwithnameprevious = filepathwithnameprevious.replace ('/','\\')
                filepathwithnamenew = filepathwithnamenew.replace ('/','\\')
            print (filepathwithnameprevious, ' -> ', filepathwithnamenew)
            if not simulateonly:
                if Path(filepathwithnamenew).exists(): #.is_file()
                    notrenamedduetoexistingfile += 1
                else:
                    os.rename(filepathwithnameprevious, filepathwithnamenew)

            dbConnection.execute('UPDATE ' + tablename + ' SET action = "renamed" WHERE id = ?', (row['id'], ))
            dbConnection.commit()
            filesrenamed += 1

        except FileNotFoundError as e:
            uprint ("Some error: " + str(e))
            renameErrors += 1

        processed += 1
        # added False because now expect not many renames to get value from reporting progress
        if False and (processed / filestoprocess) > (partprocessed / qtyofparts):
            partprocessed += 1
            print (strftime("%Y-%m-%d %H:%M:%S", localtime()))
            print ('part ', partprocessed, ' of ', qtyofparts, ' current item number ', processed, ' deleted: ', filesrenamed)
            print ()

    print ('Files renamed:                          ', "{:,.0f}".format(filesrenamed).replace(",", " "))
    print ('Files not renamed due to existing file: ', "{:,.0f}".format(notrenamedduetoexistingfile).replace(",", " "))
    print ('Errors:                                 ', "{:,.0f}".format(renameErrors).replace(",", " "))
    #end(startTime, dbConnection)


# ----------------------------------------------------------------------------------------------------------------------------------------------------#

# delete empty folders inside needed path
def deleteemptyfolders(pathwheretodelete):

    uprint("----- deleting empty folders in path '" + pathwheretodelete + "' -----\n" )


    dirsdeleted = 0 # in simulateonly mode will count only folders that are empty already at time of check
    dirsdeletedprev = -1 # need to check for empty again after deletion (upward can become empty)
    dirdelerrors = 0
    dirsprinted = 0

    while dirsdeletedprev < dirsdeleted:

        dirsdeletedprev = dirsdeleted

        for root, directories, filenames in os.walk(pathwheretodelete):

            # no need for pure diectory structure, only files with full paths
            for directory in directories:
                dirpath = os.path.join(root,directory)
                if os.listdir(dirpath) == []:
                    #uprint (dirpath)
                    try:
                        if not simulateonly:
                            os.rmdir(dirpath)
                        else:
                            dirsdeletedprev += 1 # to prevent infinite loop
                            if dirsprinted == 0:
                                uprint ("Found empty:")
                                dirsprinted += 1
                            uprint (dirpath)
                        #os.rename(dirpath, '1')
                        dirsdeleted += 1
                    except Exception as e:
                        uprint ("(Un?)expected error: " + str(e))
                        #print("Unexpected error:", sys.exc_info()[0])
                        dirdelerrors += 1
                else:
                    pass 
    
    if dirsprinted != 0:
        print ()                         
    print ('Deleted: ', "{:,.0f}".format(dirsdeleted).replace(",", " "))
    print ('Errors: ', "{:,.0f}".format(dirdelerrors).replace(",", " "))
    print ()
    #end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------#

# reading files info from a path and insert to database

def addfiles(disk_name, files_path, tablename, checksame):
    
    uprint("----- adding files from '" + files_path + "' for disk '" + disk_name + "' -----\n" )  
    # use to make tables in new database'
    make_db_table (dbConnection, tablename)

    if checksame == True:
        add_index_db_table_filepath (dbConnection, tablename)

    processed_files = 0
    processed_dirs = 0
    partprocessed = 0
    added_files = 0

    print ("Files to process: ", filestoprocess)
    print ()

    for root, directories, filenames in os.walk(files_path):

        for directory in directories:
            #uprint (os.path.join(root, directory))
            processed_dirs += 1

        for filename in filenames: 

            try:
                filepath = os.path.join(root,filename)    
                filepath_db = filepath[len(files_path):]
                filesfound = 0

                if checksame == True:
                    filepath_db=filepath_db.replace('"','""') # SQLite to have quotation marks in strings need to double them, need to do here as concatenate full SQLite command as a text string, no need to multiple quotation marks more below when each variable is passed separately to dbConnection.execute ('INSERT...'); P.S. to concatenate in SQLite use ||
                    c = dbConnection.execute('SELECT disk, filesize, filename, filenamenew, filepath, filetime, action, runID, sha256, sha256_start, sha256_end FROM ' + tablename + ' WHERE disk = "' + disk_name + '" AND filepath = "' + filepath_db + '"') 
                    filepath_db=filepath_db.replace('""','"') # replace back after SQLite execution
                    for row in c:
                        filesfound += 1
                        #files_size += row['filesize']
                
                if filesfound == 0:
#                    filesize = os.path.getsize(filepath)
#                    filetime = os.path.getmtime(filepath)
# changed to using lstat as code above resulted in error in case of broken symlinks 
# (I intend to read links themselves even if not broken, not where they point)
# lstat() stats files not following symlinks 
# one may use stat(follow_symlinks=False) instead [per docs, ! ver 3.10+]
                    filesize = Path(filepath).lstat().st_size
                    filetime = Path(filepath).lstat().st_mtime
                    
                    if os.path.islink(filepath):
                        # read link and calculate sha256 of it
                        linkname = os.readlink(filepath)
                        sha256_temp = hashlib.sha256(linkname.encode('utf-8')).hexdigest()
                        uprint(" - link ! ",filepath, " -> ", linkname) # just alert, links were rare
                        sha256_start = sha256_temp
                        sha256_end = sha256_temp
                    else:      
                        # read file contents and calculate sha256
                        BLOCKSIZE = 65536 # in bytes, 65536*256 ~= 16mb, * 256 increases processing time with many small files
                        hasher = hashlib.sha256() #md5
                        hasher_start = hashlib.sha256()
                        hasher_end = hashlib.sha256()
                        with open(filepath, 'rb') as afile:
                            buf = afile.read(BLOCKSIZE)
                            hasher_start.update(buf)
                            buf_last = buf # added for case of 0 length files
                            while len(buf) > 0:
                                hasher.update(buf)
                                buf_last = buf
                                buf = afile.read(BLOCKSIZE)
                        #buf_last = buf
                        hasher_end.update(buf_last)
                        sha256_temp = hasher.hexdigest()
                        sha256_start = hasher_start.hexdigest()
                        sha256_end = hasher_end.hexdigest()
                        #uprint(hasher.hexdigest())

            #        filepath = filepath [len(diskMount):].replace ('/', '\\') # as paths are for stored in Windows format, need to change path format that was read in Linux (long since changed that)

                    dbConnection.execute('INSERT INTO ' + tablename + ' (disk, filename, filepath, filesize, filetime, sha256, sha256_start, sha256_end) VALUES (?,?,?,?, CAST(? AS INTEGER),?,?,?)', (disk_name, filename, filepath_db, filesize, filetime, sha256_temp, sha256_start, sha256_end))
                    added_files += 1

                #dbConnection.commit()

                processed_files += 1

                processed = (processed_files if systemType == 'Windows' else (processed_files + processed_dirs)) # standard file manager in Windows shows number of files and folders separately when property of location is opened, on Unix total number of objects is shown, so it is easier to use this way when setting QTY parameter for script

                if (processed / filestoprocess) > (partprocessed / qtyofparts):
                    dbConnection.commit() # commit each part
                    partprocessed += 1
                    print (strftime("%Y-%m-%d %H:%M:%S", localtime()))
                    print ('part ', partprocessed, ' of ', qtyofparts, ' current item number: ', processed)
                    print ()

            except Exception as e:
                uprint ("(Un?)expected error: " + str(e))

    dbConnection.commit()

    print ('Number of files   that have been added : {:,.0f}'.format(added_files).replace(',', ' '))        
    print ('Number of files   that have been read  : {:,.0f}'.format(processed_files).replace(',', ' '))
    print ('Number of folders that have been read  : {:,.0f}'.format(processed_dirs).replace(',', ' '))
    print ('Sum    of objects that have been read  : {:,.0f}'.format(processed_dirs + processed_files).replace(',', ' '))
    print ()

# ----------------------------------------------------------------------------------------------------------------------------------------------------#

if MainAction == 'read':
    # reading files info to database
    addfiles (diskname, full_path, tablename_main, False)
    print ()
    end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------

if MainAction == 'add':
    # reading/adding new files to database
    addfiles (diskname, full_path, tablename_main, True)
    end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------
#
# delete files in path if same file is found in database or other path (one and only choice should be supplied)

if MainAction == 'delete':
    if (full_path != None): # ^ (dblocation == './temp.db') 
        print ('-reading files to against which to check to temp database main table')
        delete_db_table (dbConnection, tablename_main) # detele previous reads if existed 
        addfiles (diskname, full_path, tablename_main, False)
        print ()

    # reading files to possibly delete later in case duplicates are found in database
    delete_db_table (dbConnection, tablename_temp) # detele previous reads if existed 
    addfiles (diskname, full_path_d, tablename_temp, False)

    # comparing files and marking found duplicates for deletion
    comparefiles (tablename_temp, tablename_main)

    # deleting files found and marked to be deleted
    deletefiles (full_path_d,tablename_temp) # works only if in comparefiles() deletion was marked in where to find, not found (deleteInFoundNotInFind = False)

    # deleting empty folders
    deleteemptyfolders (full_path_d)

    if rename_files:
        print ('-renaming files')
        renamefiles (full_path_d, tablename_main)
        print ()
    end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------
#
# delete top folders in path if same folder with full matched contents is found in database or other path (one and only choice should be supplied)

if MainAction == 'deletefolders':
    if (full_path != None): # ^ (dblocation == './temp.db')
        print ('-reading files to against which to check to temp database main table')
        delete_db_table (dbConnection, tablename_main) # detele previous reads if existed
        addfiles (diskname, full_path, tablename_main, False)
        print ()

    # reading files to possibly delete later in case duplicates are found in database
    delete_db_table (dbConnection, tablename_temp) # detele previous reads if existed
    addfiles (diskname, full_path_d, tablename_temp, False)

    # comparing folders and marking found duplicates files for fully matched folders for deletion
    comparefolders (tablename_temp, tablename_main)

    # deleting files found and marked to be deleted
    deletefiles (full_path_d, tablename_temp) # works as in comparefoders() deletion is coded to be marked in where to find

    # deleting empty folders, this might be questionable, however here as IIRC no software project file structure has required empty folders 
    deleteemptyfolders (full_path_d)

    end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------
#
# delete and rename files marked already in database

if MainAction == 'deletemarked':
    print ('-deleting files found and marked to be deleted')
    deletefiles (full_path_d, tablename_temp) # works only if in comparefiles() deletion was marked in where to find, not found (deleteInFoundNotInFind = False)
    print ()
    print ('-deleting empty folders')
    deleteemptyfolders (full_path_d)
    print ()
    if rename_files:
        print ('-renaming files')
        renamefiles (full_path_d, tablename_main)
        print ()
    end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------
#
# search for duplicates between two tables in database

if MainAction == 'compareonly':
    print ('-comparing files and marking found duplicates for deletion')
    comparefiles (tablename_temp, tablename_main)
    print ()
    end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------#
#
# compare entries in database tables (made from files in folders)

def markduplicates (tablemain):

    # looks through files in database in table tablemain and searches for similar files in same table - with and same filesize, sha256, same name (depends on filenamematchway), size, modification date (depends on checkTime) 
    # if 'same' file found script marks file as 'todelete'

    runID = 'pc 1'
    deletionFilter = '(action <> "deleted" AND action <> "todelete" OR action is NULL)'

    checkSize = 1 # if 1 exact match by size, otherwise as fraction needed for match - usefull for mp3 as metadata- not used now

    processed = 0
    partprocessed = 0
    recommended_for_deletion = 0
    recommendedNameChange = 0

    myQuery = 'SELECT id, disk, filename, filenamenew, filepath, filesize, filetime, sha256, sha256_start, sha256_end FROM ' + tablemain + ' WHERE ' + deletionFilter + ' ORDER BY filesize DESC, sha256 ASC, filename DESC, filetime DESC'

    c_find = dbConnection.execute(myQuery)

    file_size_prev       = None
    file_sha256_prev     = None
    file_name_prev       = None
    file_time_prev       = None

    for row in c_find:

        file_size       = row['filesize']
        file_sha256     = row['sha256']
        file_name       = row['filename']
        file_time       = row['filetime']

        match_found = False
        if file_size_prev == file_size:
            if file_sha256_prev == file_sha256:
                if not checkTime or file_time_prev == file_time:
                    if not (filenamematchway == 'matchfilenamesexactly') or file_name_prev == file_name:
                        match_found = True

        file_size_prev       = file_size
        file_sha256_prev     = file_sha256
        file_name_prev       = file_name
        file_time_prev       = file_time

        if match_found:
            # mark for deletion
            try:
                dbConnection.execute('UPDATE ' + tablemain + ' SET runID = ?, action = "todelete" WHERE id = ?',(runID, row['id'], ))
                recommended_for_deletion += 1                
            except Exception as e:
                uprint ("(Un?)expected error: " + str(e))
                uprint ("Database error on:", 'id: ', rowToFind['id'], 'filename: ', rowToFind['filename'], 'filesize: ', rowToFind['filesize'], 'filetime: ', rowToFind['filetime'])

        processed += 1
        if (processed / filestoprocess) > (partprocessed / qtyofparts):
            partprocessed += 1
            print (strftime("%Y-%m-%d %H:%M:%S", localtime()))
            print ('part ', partprocessed, ' of ', qtyofparts, ' current item number ', processed, ' marked for deletion: ', recommended_for_deletion)
            print ()

    dbConnection.commit()
    print ('Files marked for deletion: ', "{:,.0f}".format(recommended_for_deletion).replace(",", " "), ' out of:', "{:,.0f}".format(processed).replace(",", " "))
    print ('Recommended for name change: ', "{:,.0f}".format(recommendedNameChange).replace(",", " "))
    print ()

# ----------------------------------------------------------------------------------------------------------------------------------------------------
#
# delete extra/duplicates files in path if same file is found in the same path

if MainAction == 'deletesame':

    print ('-reading files to database main table')
    delete_db_table (dbConnection, tablename_temp) # detele previous reads if existed 
    addfiles (diskname, full_path_d, tablename_temp, False)
    print ()

    print ('-comparing files and marking found duplicates for deletion')
    markduplicates (tablename_temp)
    print ()
    
    print ('-deleting files found and marked to be deleted')
    deletefiles (full_path_d,tablename_temp)
    print ()

    print ('-deleting empty folders')
    deleteemptyfolders (full_path_d)
    print ()

    end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------
#
# sync two locations, update db
# sync one way finalises with db update so to avoid double read of 2nd location call addfiles for both actions before calling functions 

# assumes initial read of new files for source_disk
def sync_one_way(source_path, source_disk, dest_path, dest_disk):

    uprint("----- sync from '" + source_path + "' to '" + dest_path + "' -----\n")
    uprint("updating table  '" + tablename_main + "' setting action='tocopy' for files present in source but absent in destination (not same full path and checksum)\n")

    # trying to make queries safe against injection attack, %s for MySQL and Postgre, ? for SQLite (for SQLite table name substition with ? resulted in error)
    # last extra sometimes unneeded binding is not OK too (so FileNameEx is not a binding too): "Incorrect number of bindings supplied. The current statement uses 3, and there are 4 supplied." (One might have two lines with execute or write '%' (any) in FileNameEx to possibly reduce SQL efficiency as workarounds.)

    c = dbConnection.execute('UPDATE "' + tablename_main + '" set action=null where (action="tocopy" or action="copied") AND disk = ?', (source_disk,))
    c = dbConnection.execute('UPDATE "' + tablename_main + '" set action="tocopy" where id not in (select DISTINCT a.id from "' + tablename_main + '" as a join "' + tablename_main + '" as b on a.filepath=b.filepath where a.sha256=b.sha256 and a.disk=? and b.disk=?) and disk=?'+ ('' if (FileNameEx is None) else ' and filepath like "' + FileNameEx + '"'), (source_disk,dest_disk,source_disk,))
    dbConnection.commit()

    copymarked(source_disk,source_path,dest_path)
    addfiles(dest_disk, dest_path, tablename_main, True)

    uprint("Checking correctness of copy...")
    c = dbConnection.execute('UPDATE "' + tablename_main + '" set action="copied_wrong_sha" where id not in (select DISTINCT a.id from "' + tablename_main + '" as a join "' + tablename_main + '" as b on a.filepath=b.filepath where a.sha256=b.sha256 and a.disk=? and b.disk=?) and disk=? and action="copied"', (source_disk,dest_disk,source_disk,))
    dbConnection.commit()

    c = dbConnection.execute('select count(*) as qty_found_errors from "' + tablename_main + '" where action="copied_wrong_sha" AND disk = ?',(source_disk,))
    row = c.fetchone()
    qty_err = row['qty_found_errors']
    if qty_err == 0:
        uprint("No errors found\n")
    else:
        uprint(" ! Number of found errors : {:,.0f}".format(qty_err).replace(',', ' '))
        uprint("To see: database, entries with 'copied_wrong_sha' in action field.\n")


# assumes initial read of new files for source_disk
def sync_two_ways(path1, disk1, path2, disk2):
    sync_one_way(path1, disk1, path2, disk2)
    sync_one_way(path2, disk2, path1, disk1)

# ----- #

# sync two ways
if MainAction == 'sync2':
    addfiles(diskname, full_path, tablename_main, True)
    sync_two_ways(full_path, diskname, full_path_c, diskname_c)
    end(startTime, dbConnection)

# sync one way
if MainAction == 'sync':
    addfiles(diskname, full_path, tablename_main, True)
    sync_one_way(full_path, diskname, full_path_c, diskname_c)
    end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------
#
# SOME OTHER USEFUL CODE


# db location recommended to put on RAM memory drive, highly was before commmit changed to be made for many transactions at one, not for each other
# RAM disk on Linux
# sudo mkdir -p /media/ramdisk
# sudo mount -t tmpfs -o size=2048M tmpfs /media/ramdisk
# OR
# sudo mount -t tmpfs -o size=50%,X-mount.mkdir tmpfs /media/ramdisk
# sudo umount /media/ramdisk

# https://stackoverflow.com/questions/36376844/replace-with-in-python-3
# https://stackoverflow.com/questions/2081640/what-exactly-do-u-and-r-string-flags-do-in-python-and-what-are-raw-string-l
# use raw strings


# diskMount = r'J:/' if systemType == 'Windows' else r'/media/alex/4tb-57/'
# diskMount = r'/media/alex/4tb-57/' if systemType == 'Linux' else r'J:/' 

#How to get table names using sqlite3 through python?
#res = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
#for name in res:
#    print name[0]


    '''srcfile = '/home/alex/Documents/python/test.py'
    dstfile = '/home/alex/Documents/python/1/1/1/12341/1hkl/1/132543243/test.py'

    print (os.path.isabs(srcfile))
    #assert os.path.isabs(srcfile)
    dstdir =  os.path.dirname(dstfile)

    os.makedirs(dstdir) # create all directories, raise an error if it already exists
    shutil.copyfile2(srcfile, dstfile)

    end(startTime, dbConnection)'''


