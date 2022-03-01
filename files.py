#!/usr/bin/python3

# Python 3.5 tested

# version 4.0 2021 April 11
# added makedirs command - create empty directory structure from database entries, see help for more info

# version 3.9 2021 March 31
# added add_index_db_table to files_functions.py module and call it here to speed up comparing entries about 1000 (1k) times in case of large data sets

# version 3.8 2021 march 23
# fixed error on checktime, which was false always, changed to canonic way per https://stackoverflow.com/questions/15008758/parsing-boolean-values-with-argparse 

# version 3.7
# fixed checktime error: effect was opposite of planned, added "not" to 
# checkTime = not (args.notchecktime or args.notmatchtime)
# strage how I did not see it before...maybe if flipped on some version

# version 3.6 / 2021 March
# added --notmarchtime, changed/added some comments
# changed deletesame to work with files_d and tablename_temp instead of files and tablename_main (would be easier to use db after for deletemarked as would use same table)

# version 3.5 / 2021 Feb
# implemented search for action filled-in during previous runs (--action)

# version 3.4 / 2021 Jan
# implemented command deletesame - delete dublicates in same location (--files) by filesize, sha256; also by name (exact or not, partial matching option same effect as do not match) and timestamp (exact ot not)

# version 3.3
# delete works with db and now also other pata
# TO DO test renaming function

# version 3.2 
# deletemarked function is changed to need diskname (default diskname stored when not supplied during reading is "temp" and it is set automatically to that for deletesame) as a safeguard and to work with partial path from database adding files_d parameter at beginning for path;
# all functions changed to use partial paths - excluding path to whole structure, w/out leading /, so it is expected to supply paths ending with / for commands like "deletemarked")
# deletion works with --files_d parameter now, not --files

# version 3.1
# copy files from one location - (files parameter) to other (files_c parameter) for those files where action field in db is set to 'to copy' from specific disk (disk parameter)
# TO DO - in db paths are now stored not full when reading, but only part w/out path to - so need for change code here (DONE in 3.2)

# read files properties from set new and old folders, writing info into database
# looks through files in one folder and searches for files with same (or similar) name, size, hash sha256 and modification date in other folder, if same file found script can delete file in first or second folder.
# reads db with file properties for information search and totals

# TO DO:
# 1. notUsefulEnd - change from list of items [] to any combination of items from list
# 2. implement deleting directly location against location - DONE ver 3.3
# 3. test on deleting from path which contained in database - dublicates inside one location, not one against the other
# 4. check on that:
# if updateDuringSearch:
# dbConnection.commit() needed if search for dublicates is made with single set of files, not one against the other; 
# changes results of outer select for FOR and slower than commit at the end; 
# may change outer query qty returned as working due to SQLite functioning, so be careful when searcing single set of data, not one against the other

from files_functions import uprint, end, make_db_table, delete_db_table, add_index_db_table
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
#from argparse import ArgumentParser

import argparse
from argparse import RawTextHelpFormatter
parser = argparse.ArgumentParser(description='Process file structures, deleting dublicates renaming retained files is useful if additional info is not contained in extention - part of file name after last . symbol; paths better be passed as absolute', formatter_class=RawTextHelpFormatter)
parser.add_argument('command', choices=['read', 'search','totals', 'delete', 'deletemarked', 'compareonly', 'change', 'copy', 'deletesame', 'makedirs'], help='command name: \ndelete - deletes files in path (--files_d) against database (--db) or other path (--files) by sha256 and only if file is found on each of all disks (--disks can be several times), also --notchecktime --mne --mnb --nmn --rename optional), \ndeletemarked - deleting (and renaming) what is marked already in database (by action field set to "todelete"; if need to redo deletion for another disk, please run "change" to semi-manually change action field) and --files_d is used to add to path stored in database at beginning and --disk is used to delete marked for that disk only as a safeguard, delete from temp table, rename what is in main table, \ncompareonly - run only matching procedure for two tables in database which should be filled in already, \ncopy - copy files from one location (--files) to other (--files_c) for those files where action field in database (--db) is set to "to copy" for specific --disk, \ndeletesame - delete dublicates in same location (--files_d) by filesize, sha256; also by name (exact or not, partial matching option same effect as do not match) and timestamp (exact ot not), \nmakedirs - make directories in path of files_c from filesdata entries in database')
parser.add_argument('--db', default='./temp.db', help='full path to database location, default = temp.db in current folder')
parser.add_argument('--files', help='full path to the only/main file structure')
parser.add_argument('--files_d', help='full path to other file structure - where objects need to be deleted')
parser.add_argument('--files_c', help='full path to other file structure - whereto objects need to be copied for copy/or folders be created for makedirs')
parser.add_argument('--disk', help='disk name tag of file structure info - for read, totals, search')
parser.add_argument('--disks', action='append', help='disk name tags when searched for candidates for deletion, if present, file should be present on all disks in main table to be considered a candidate, if omitted, should be present in main table as a whole. Should be one name per argument, several arguments possible, NOT several in one argument separated by comma')
parser.add_argument('--pattern', help='filename expression to search, percentage sign symbol can be used as any number of any symbols, ignore case, _ symbol means any AFAIK, for exact search add --exact parameter') # symbol % in help string gives argparse error on parse_args() line
parser.add_argument('--action', help='action text to search, usefull after processing, e.g. set to "deleted" if deleted') 

parser.add_argument('--notchecktime', action='store_false', dest='checkTime', help='when looking for dublicates, do not check that timestamp (modification time) is the same, default = check time')
parser.add_argument('--notmatchtime', action='store_false', dest='checkTime', help='when looking for dublicates, do not check that timestamp (modification time) is the same, default = check time, same effect as notchecktime')
parser.set_defaults(checkTime=True)
parser.add_argument('--mne', dest='filenamematchway', action='store_const', const='matchfilenamesexactly', default = 'matchfilenamesexactly', help='when looking for dublicates, to match file names exactly, this is default')
parser.add_argument('--mnb', dest='filenamematchway', action='store_const', const='matchfilenambeginnings', help='when looking for dublicates, to match file names where one name begins with full other name (w/out extention)')
parser.add_argument('--nmn', dest='filenamematchway', action='store_const', const='notmatchfilenames', help='when looking for dublicates, do not check file names, by other file data only')
parser.add_argument('--simulateonly', action='store_true', help='do not actually delete files on disk, action is db is set to "deleted" still') # defaults to opposite of action if action='store_true' or 'store_false'
parser.add_argument('--tmp', action='store_true', help='for search and totals - use tmp table in db, default - main table')
parser.add_argument('--exact', action='store_true', help='for search - use exact filename match, default - LIKE clause for SQL')
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
disks = args.disks
FileNameEx = args.pattern
filestoprocess = args.qty
qtyofparts = args.parts
filenamematchway = args.filenamematchway
simulateonly = args.simulateonly
checkTime = args.checkTime
rename_files = args.rename
#print (rename_files)
#exit()


use_temp_table = args.tmp
exact_search = args.exact
action = args.action

if disks == None:
    disks =[None]

if MainAction in ['delete', 'deletesame']:
    diskname = 'temp'

#qw = 'err'
#disks += [qw]
#print (len(disks))
#print (disks)
#end()
togo = True

if full_path != None and full_path == full_path_d:
    print ('- paths to files same: files and files_d, terminating')
    togo = False

if MainAction in ['search', 'totals', 'change', 'copy', 'deletemarked', 'makedirs'] and dblocation == './temp.db':
    print ('- path to database ("--db") is required for this command, if you want to use ./temp.db, please give another path version to it')
    togo = False

if MainAction in ['search'] and FileNameEx == None:
    print ('- search pattern ("--pattern") is required for this command')
    togo = False

if MainAction in ['read', 'clean', 'copy'] and full_path == None:
    print ('- path to file structure ("--files") is required for this command')
    togo = False

if MainAction in ['copy', 'makedirs'] and full_path_c == None:
    print ('- path to second file structure (whereto copy - "--files_c") is required for this command')
    togo = False

if MainAction in ['copy', 'deletemarked', 'makedirs'] and diskname == None:
    print ('- diskname ("--disk") is required for this command')
    togo = False
 
if MainAction in ['delete', 'deletemarked', 'deletesame'] and full_path_d == None:
    print ('- path to file structure where deletion is expected (--files_d) is required for this command')
    togo = False

if MainAction in ['delete'] and not ((full_path == None) ^ (dblocation == './temp.db')): # ^ is logical xor here
    print ('- either path to file structure (--files) or database (--db) to check againt is required for this command (not both though)')
    togo = False

if togo == False:
    print ('Next run please take into account remarks listed above') 
    end()

tablename_main = 'filesdata'
tablename_temp = 'filesdata_todelete'
    
# https://stackoverflow.com/questions/110362/how-can-i-find-the-current-os-in-python
systemType = platform.system()
if dblocation == './temp.db':
    if systemType == 'Windows':
        dblocation = 'temp.db' # r'a:/listfiles.db'.replace ('\\','/')
    elif systemType == 'Linux':
        pass # dblocation = r'/media/alex/4tb-57/listfiles 4tb-57 by Ub 18 studio.db'
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
print ()
startTime = datetime.today()

# ----------------------------------------------------------------------------------------------------------------------------------------------------#
# to copy files set to be copied (field 'action' is 'to copy') - see help for detailes

if MainAction == 'copy':

    '''srcfile = '/home/alex/Documents/python/test.py'
    dstfile = '/home/alex/Documents/python/1/1/1/12341/1hkl/1/132543243/test.py'

    print (os.path.isabs(srcfile))
    #assert os.path.isabs(srcfile)
    dstdir =  os.path.dirname(dstfile)

    os.makedirs(dstdir) # create all directories, raise an error if it already exists
    shutil.copyfile2(srcfile, dstfile)

    end(startTime, dbConnection)'''

    notcopiedasnotfound = 0
    notcopiedasfailed = 0
    notcopiedaswerethere = 0
    copied = 0

    c = dbConnection.execute('select id, filepath from filesdata where action="to copy" AND disk = "' + diskname + '"')
    for row_c in c:
        #print(row_c['filepath'])
        srcfile = full_path + row_c['filepath']
        if not Path(srcfile).exists(): #.is_file()
            uprint ('not found: ',srcfile)
            notcopiedasnotfound += 1
            d = dbConnection.execute('UPDATE filesdata SET action = "not copied as not found" WHERE id = ' + str(row_c['id']))
        else:
            dstfile = full_path_c + row_c['filepath']
            if Path(dstfile).exists(): #.is_file()
                uprint ('already there: ',dstfile)
                notcopiedaswerethere += 1
                d = dbConnection.execute('UPDATE filesdata SET action = "not copied as was already there" WHERE id = ' + str(row_c['id']))
            else:
                dstdir =  os.path.dirname(dstfile)
                try:
                    os.makedirs(dstdir) # create all directories, raise an error if it already exists
                except Exception as e:
                    pass
                try:
                    shutil.copy2(srcfile, dstdir)
                    copied +=1
                    d = dbConnection.execute('UPDATE filesdata SET action = "copied" WHERE id = ' + str(row_c['id']))
                except Exception as e:
                    notcopiedasfailed +=1
                    d = dbConnection.execute('UPDATE filesdata SET action = "not copied as copy failed" WHERE id = ' + str(row_c['id']))

    dbConnection.commit()

    print ('Number of files that have been copied         : {:,.0f}'.format(copied).replace(',', ' '))
    print ('Number of files that have been not been found : {:,.0f}'.format(notcopiedasnotfound).replace(',', ' '))
    print ('Number of files that have been there already  : {:,.0f}'.format(notcopiedaswerethere).replace(',', ' '))
    print ('Number of files that had errors on copy       : {:,.0f}'.format(notcopiedasfailed).replace(',', ' '))

    end(startTime, dbConnection)


# ----------------------------------------------------------------------------------------------------------------------------------------------------#
# to copy files set to be copied (field 'action' is 'to copy') - see help for detailes

if MainAction == 'makedirs':

    c = dbConnection.execute('select id, filepath from filesdata where disk = "' + diskname + '"')
    for row_c in c:
        dstfile = full_path_c + row_c['filepath']
        dstdir =  os.path.dirname(dstfile)
        try:
            os.makedirs(dstdir) # create all directories, raise an error if it already exists
        except Exception as e:
            pass

    end(startTime, dbConnection)


# ----------------------------------------------------------------------------------------------------------------------------------------------------#
# to change info in database - maybe usefull if SQL tool is not used

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

        c = dbConnection.execute('SELECT disk, action, sum (filesize) as Total, count (filesize) as Qty FROM filesdata WHERE (action <> "SOMETHINGdeleted" OR action IS NULL)' + ('' if (row_d['disk'] is None) else ' AND disk = "' + row_d['disk'] + '"') + ' GROUP BY action')

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

    # c = dbConnection.execute('SELECT disk, filesize, filename, filenamenew, filepath, filetime, action, runID FROM filesdata WHERE disk = "Test" AND filepath LIKE "%alex%" AND filename LIKE "%a%" ORDER BY filepath ASC') 
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

def comparefiles (tablepossibledublicates, tablemain):

    # looks through files in database in table tablepossibledublicates and searches for similar files in table tablemain - with same name (depends on filenamematchway), size, modification date (depends on checkTime) and same sha256
    # if 'same' file found script marks file from tablepossibledublicates as 'todelete'
    # making for deletion is done only if file is found on each of all disks ([disks] variable)

    # added to speed up search many times in case of large number of entries to matches against
    add_index_db_table (dbConnection, tablemain)
    
    runID = 'pc 1'
    deletionFilter = '(action <> "deleted" AND action <> "todelete" OR action is NULL)'

    # if folders overlap, need to update database continuously else dublicates will be deleted both - NOT APPLICABLE HERE, HENCE FALSE
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

    myQuery = 'SELECT id, disk, filename, filenamenew, filepath, filesize, filetime, sha256, sha256_start, sha256_end FROM ' + tablepossibledublicates + ' WHERE ' + deletionFilter

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
                                    dbConnection.execute('UPDATE ' + (tablepossibledublicates if deleteInFoundNotInFind else tablemain) + ' SET runID = ?, filenamenew = ? WHERE id = ?',(runID, new_file_name, rowToUpdate, ))
                                    dbConnection.execute('UPDATE ' + (tablepossibledublicates if deleteInFoundNotInFind else tablemain) + ' SET runID = ?, action = "toupdate" WHERE id = ?',(runID, rowToUpdate, ))
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
                dbConnection.execute('UPDATE ' + (tablepossibledublicates if not deleteInFoundNotInFind else tablemain) + ' SET runID = ?, action = "todelete" WHERE id = ?',(runID, rowToDelete, ))
                recommended_for_deletion += 1
            
                if updateDuringSearch:
                    dbConnection.commit() # needed if search for dublicates is made with single set of files, not one against the other; changes results of outer select for FOR and slower than commit at the end; may change outer query qty returned as working due to SQLite functioning, so be careful when searcing single set of data, not one against the other
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
    #end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------#
#
# delete files marked for deletion
def deletefiles(targetpath, tablename):

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
    #end(startTime, dbConnection)


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
def deletefolders(pathwheretodelete):

    dirsdeleted = 0
    dirsdeletedprev = -1 # need to check for empty again after deletion (upward can become empty)
    dirdelerrors = 0

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
                        #os.rename(dirpath, '1')
                        dirsdeleted += 1
                    except Exception as e:
                        uprint ("(Un?)expected error: " + str(e))
                        #print("Unexpected error:", sys.exc_info()[0])
                        dirdelerrors += 1
                else:
                    pass 

    print ('Deleted: ', "{:,.0f}".format(dirsdeleted).replace(",", " "))
    print ('Errors: ', "{:,.0f}".format(dirdelerrors).replace(",", " "))
    #end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------#

# reading files info from a path and insert to database

def readfiles(files_path, tablename):
    
    # use to make tables in new database
    make_db_table (dbConnection, tablename)

    processed_files = 0
    processed_dirs = 0
    partprocessed = 0

    print ("Files to process: ", filestoprocess)
    print ()

    for root, directories, filenames in os.walk(files_path):

        for directory in directories:
            #uprint (os.path.join(root, directory))
            processed_dirs += 1

        for filename in filenames: 

            try:
                filesize = os.path.getsize(os.path.join(root,filename))
                filetime = os.path.getmtime(os.path.join(root,filename))
                filepath = os.path.join(root,filename)

                # read sha256
                BLOCKSIZE = 65536 # in bytes, 65536*256 ~= 16mb, * 256 increases processing time with many small files
                #BLOCKSIZE_start = 
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
                #print(hasher.hexdigest())

        #        filepath = filepath [len(diskMount):].replace ('/', '\\') # as paths are for stored in Windows format, need to change path format that was read in Linux

                #uprint (os.path.join(root,filename))
                #time.sleep (1)

                # dbConnection.execute('INSERT INTO filesdata (disk, filename, filepath, filesize, filetime) VALUES ("' + diskname + '", "' + filename + '", "' + filepath + '", ' + str(filesize) + ', ' + str(CAST(filetime AS INTEGER)) + ')')
                dbConnection.execute('INSERT INTO ' + tablename + ' (disk, filename, filepath, filesize, filetime, sha256, sha256_start, sha256_end) VALUES (?,?,?,?, CAST(? AS INTEGER),?,?,?)', (diskname, filename, filepath[len(files_path):], filesize, filetime, sha256_temp, sha256_start, sha256_end))

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
    print ('Number of folders that have been read : {:,.0f}'.format(processed_dirs).replace(',', ' '))
    print ('Number of files   that have been read : {:,.0f}'.format(processed_files).replace(',', ' '))
    print ('Sum    of objects that have been read : {:,.0f}'.format(processed_dirs + processed_files).replace(',', ' '))
    #end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------#

if MainAction == 'read':
    print ('-reading files to database')
    readfiles (full_path, tablename_main)
    print ()
    end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------
#
# delete files in path if same file is found in database or other path (one and only choice should be supplied)

if MainAction == 'delete':
#heretocheck
    if (full_path != None): # ^ (dblocation == './temp.db') 
        print ('-reading files to agains which to check to temp database main table')
        delete_db_table (dbConnection, tablename_main) # detele previous reads if existed 
        readfiles (full_path, tablename_main)
        print ()

    print ('-reading files to possibly delete later in case dublicates are found in database')
    delete_db_table (dbConnection, tablename_temp) # detele previous reads if existed 
    readfiles (full_path_d, tablename_temp)
    print ()
    print ('-comparing files and marking found dublicates for deletion')
    comparefiles (tablename_temp, tablename_main)
    print ()
    print ('-deleting files found and marked to be deleted')
    deletefiles (full_path_d,tablename_temp) # works only if in comparefiles() deletion was marked in where to find, not found (deleteInFoundNotInFind = False)
    print ()
    print ('-deleting empty folders')
    deletefolders (full_path_d)
    print ()
    if rename_files:
        print ('-renaming files')
        renamefiles (full_path_d, tablename_main)
        print ()
    end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------
#
# delete and rename files marked already in database

if MainAction == 'deletemarked':
    print ('-deleting files found and marked to be deleted')
    deletefiles (full_path_d, tablename_temp) # works only if in comparefiles() deletion was marked in where to find, not found (deleteInFoundNotInFind = False)
    print ()
    print ('-deleting empty folders')
    deletefolders (full_path_d)
    print ()
    if rename_files:
        print ('-renaming files')
        renamefiles (full_path_d, tablename_main)
        print ()
    end(startTime, dbConnection)

# ----------------------------------------------------------------------------------------------------------------------------------------------------
#
# search for dublicates between two tables in database

if MainAction == 'compareonly':
    print ('-comparing files and marking found dublicates for deletion')
    comparefiles (tablename_temp, tablename_main)
    print ()
    end(startTime, dbConnection)


# ----------------------------------------------------------------------------------------------------------------------------------------------------#
#
# compare entries in database tables (made from files in folders)

def markdublicates (tablemain):

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
    #end(startTime, dbConnection)



# ----------------------------------------------------------------------------------------------------------------------------------------------------
#
# delete extra/dublicates files in path if same file is found in the same path

if MainAction == 'deletesame':

    print ('-reading files to database main table')
    delete_db_table (dbConnection, tablename_temp) # detele previous reads if existed 
    readfiles (full_path_d, tablename_temp)
    print ()

    print ('-comparing files and marking found dublicates for deletion')
    markdublicates (tablename_temp)
    print ()
    
    print ('-deleting files found and marked to be deleted')
    deletefiles (full_path_d,tablename_temp)
    print ()

    print ('-deleting empty folders')
    deletefolders (full_path_d)
    print ()

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



