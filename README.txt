### Python script files.py for identifying duplicates of files, removing them and some other tasks on folders with files.
#### files_functions.py includes some functions used by files.py

##### Copyright (c) 2009-2023, Alex Martian. All rights reserved.

#### Licences: GNU GPL 2.0, GNU GPL 3.0.

Python 3.8, Linux Mint 20.2/21 tested
Only some earlier versions IIRC were run on Windows, it might still work or require at least minor changes to paths (/ vs \).

##### Below is output of `./files.py --help` as pasted unformatted into text editor (might look better as bash output in terminal):

usage: files.py [-h] [--version] [--verbose] [--db DB] [--files FILES]
                [--files_d FILES_D] [--files_c FILES_C] [--disk DISK]
                [--disk_c DISK_C] [--disks DISKS] [--pattern PATTERN]
                [--action ACTION] [--notchecktime] [--notmatchtime] [--mne]
                [--mnb] [--nmn] [--simulateonly] [--tmp] [--exact] [--rename]
                [--qty QTY] [--parts PARTS]
                {read,add,search,totals,delete,deletemarked,compareonly,change,copy,deletesame,makedirs,sync,sync2,deletefolders}

Process file structures, deleting duplicates renaming retained files is useful if additional info is not contained in extention - part of file name after last . symbol; paths better be passed as absolute

positional arguments:
  {read,add,search,totals,delete,deletemarked,compareonly,change,copy,deletesame,makedirs,sync,sync2,deletefolders}
                        command name: 
                        read - adds files in --filespath to database --db (modification date, size, sha256sum, path, name, --disk), 
                        adds - same as read but adds only those that are not already in --db (checks for same --disk AND path that includes name), 
                        search - outputs found files and info on them, 
                        totals - outputs totals, 
                        delete - deletes files in path (--files_d) against database (--db) or other path (--files) by file sha256, name, size and modification time and only if file is found on each of all disks (--disks can be several times), also --notchecktime --mne --mnb --nmn --rename (optional), 
                        deletemarked - deleting (and renaming) what is marked already in database (by action field set to "todelete" in files_todelete table; if need to redo deletion for another disk, please run "change" to semi-manually change action field) and --files_d is used to add to path stored in database at beginning and --disk is used to delete marked for that disk only as a safeguard, delete from temp table, rename what is in main table, 
                        compareonly - run only matching procedure for two tables in database which should be filled in already, 
                        copy - copy files from one location (--files) to other (--files_c) for those files where action field in database (--db) is set to "tocopy" for specific --disk, 
                        deletesame - delete duplicates in same location (--files_d) by filesize, sha256; also by name (exact or not, partial matching option same effect as do not match) and timestamp (exact ot not), 
                        makedirs - make directories in path of files_c from filesdata entries in database, 
                        sync - add files absent on one disk/location to another disk/location and update the db, need disk, disk_c - to locate files in db, files, files_c - paths to roots of locations to copy from and copy to (paths from db are appended to them), 
                        sync2 - same as sync but do both ways, from files to files_c then from files_c to files, 
                        deletefolders - delete top level folder(s) recursively in provided --files_d path if complete matched folders contents are found in database (--db) or other path (--files) by file sha256, name, size and modification time and only if file is found on each of all disks (--disks can be several times), also --notchecktime (optional)

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --verbose             output additional info, default: no output
  --db DB               full path to database location, default = temp.db in current folder
  --files FILES         full path to the only/main file structure
  --files_d FILES_D     full path to other file structure - where objects need to be deleted
  --files_c FILES_C     full path to other file structure - whereto objects need to be copied for copy/or folders be created for makedirs
  --disk DISK           disk name tag of file structure info - for add, read, totals, search, sync, sync2
  --disk_c DISK_C       disk name tag to copy files to, used by sync, sync2 commands
  --disks DISKS         disk name tags when searched for candidates for deletion against database, if present, file should be present on all disks in main table to be considered a candidate, if omitted, should be present in main table as a whole. Should be one name per argument, several arguments possible, NOT several in one argument separated by comma
  --pattern PATTERN     filename expression to search, percentage sign symbol can be used as any number of any symbols, ignore case, _ symbol means any AFAIK, for exact search add --exact parameter
  --action ACTION       action text to search, usefull after processing, e.g. set to "deleted" if deleted
  --notchecktime        when looking for duplicates, do not check that timestamp (modification time) is the same, default: check time
  --notmatchtime        when looking for duplicates, do not check that timestamp (modification time) is the same, default = check time, same effect as notchecktime
  --mne                 when looking for duplicates, to match file names exactly, this is default
  --mnb                 when looking for duplicates, to match file names where one name begins with full other name (w/out extention)
  --nmn                 when looking for duplicates, do not check file names, by other file data only
  --simulateonly        do not actually delete files on disk, action is db is set to "deleted" still
  --tmp                 for search and totals - use tmp table in db, default: main table
  --exact               for search - use exact filename match, default: LIKE clause for SQL
  --rename              rename retained files with additional potentially useful info from deleted files names, default - analyse names and store in db, do not rename on disk
  --qty QTY             number of files expected to be processed, default = 1 000 000
  --parts PARTS         how many times to report intermidiary process status, default = 100
