# Python 3.5 tested

import os
import sys
import sqlite3
import time
from time import gmtime, strftime, localtime
from datetime import datetime, timedelta
#import datetime
import time

# for possible non standerd characters use for print uprint defined below
# http://stackoverflow.com/questions/14630288/unicodeencodeerror-charmap-codec-cant-encode-character-maps-to-undefined

def uprint(*objects, sep=' ', end='\n', file=sys.stdout):
    enc = file.encoding
    if enc == 'UTF-8':
        print(*objects, sep=sep, end=end, file=file)
    else:
        f = lambda obj: str(obj).encode(enc, errors='backslashreplace').decode(enc)
        print(*map(f, objects), sep=sep, end=end, file=file)

# procedure for end script execution
def end(startTime = None, dbConnection = None):
    if dbConnection is not None:
        dbConnection.close()

    print ('End:')
    print (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
    print ()
    # print (strftime("%Y-%m-%d %H:%M:%S", localtime())) # strftime from time don't support milliseconds by %f
    if startTime is not None:
        print ('Duration:')
        print (str(timedelta(seconds=datetime.today().timestamp() - startTime.timestamp()))) # displays days too if > 24 hours

#other methods are my previous attempts 
#        print (time.strftime("%H:%M:%S", time.gmtime(datetime.today().timestamp() - startTime.timestamp())))
#        duration = datetime.utcfromtimestamp(24*3600+ datetime.today().timestamp() - startTime.timestamp())
#        print (duration.strftime("%Y-%m-%d %H:%M:%S.%f"), ' one day added manually, zero days is 1970-01-01')
#        # print (datetime.gmtime(datetime.today().timestamp() - startTime.timestamp()).strftime("%H:%M:%S.%f"))
#        delt = datetime.today() - startTime  # + datetime.fromtimestamp(3600*25)
#        print (str(delt)) # if longer than 24 hours will print also something like 1970-01-01

#        s = datetime.today().timestamp() - startTime.timestamp()
#        hours, remainder = divmod(s, 3600)
#        minutes, seconds = divmod(remainder, 60)
#        print ('%s:%s:%s' % (hours, minutes, seconds))

    print ()
    exit()

def make_db_table (dbconnection, tablename):
# !!!!!!!!!!!!!!!!! INTEGER may store float, and it happened with filetime, so CAST(filetime AS INTEGER) is 'better' used in selects
# https://www.sqlite.org/datatype3.html
# A CAST conversion is similar to the conversion that takes place when a column affinity is applied to a value except that with the CAST operator the conversion always takes place even if the conversion lossy and irreversible, whereas column affinity only changes the data type of a value if the change is lossless and reversible. 
    # deletion can be todelete deleted
    dbconnection.execute('CREATE TABLE IF NOT EXISTS ' + tablename +
    ''' (id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT,
    filenamenew TEXT,
    filepath TEXT,
    disk TEXT,
    filetime INT,
    filesize INT,
    action TEXT,
    runID TEXT,
    sha256 TEXT,
    sha256_start TEXT,
  sha256_end TEXT
    );''')

def delete_db_table (dbconnection, tablename):
    dbconnection.execute('DROP TABLE IF EXISTS ' + tablename)

def add_index_db_table_sha256 (dbconnection, tablename):
    dbconnection.execute('CREATE INDEX IF NOT EXISTS "sha" ON "' + tablename + '" ("sha256")')

def add_index_db_table_filepath (dbconnection, tablename):
    dbconnection.execute('CREATE INDEX IF NOT EXISTS "path" ON "' + tablename + '" ("filepath")')

# get some info on schema, not much usefull, using DB browser better
'''
cursor = dbConnection.cursor()
meta = cursor.execute("PRAGMA table_info('newfiles')")
for r in meta:
    print (r)

cursor = dbConnection.cursor()
cursor.execute("SELECT * FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())
'''


# uncomment to give sum of sizes of all distinct files in database (newfiles table)
'''
c = dbConnection.execute("SELECT sum (filesize) as Total FROM (SELECT filesize FROM newfiles GROUP BY filename, filesize, filetime)")

#myLen = len (c.fetchall())
#print (myLen)
for row in c:
    print ('Total: ')
    uprint ("{:,.0f}".format(row['Total']).replace(",", " ")) 
    print ()

dbConnection.close()
end()
'''

# uncomment to print counts (qty) across all database for 25 largest files (newfiles table)
'''
c = dbConnection.execute("SELECT filesize, filename, filepath, COUNT (filepath) AS Qty FROM newfiles GROUP BY filename, filesize, filetime ORDER BY  filesize DESC LIMIT 25")

#myLen = len (c.fetchall())
#print (myLen)
for row in c:
    print (row['filename'])
    print ('Qty: ', row['Qty'])
    uprint ("{:,.0f}".format(row['filesize']).replace(",", " ")) 
    print ()

dbConnection.close()
end()
'''

# uncomment to print counts (qty) across all database for 25 files with largest counts (newfiles table)
'''
c = dbConnection.execute("SELECT filesize, filename, filepath, COUNT (filepath) AS Qty FROM newfiles WHERE filesize > 1000000 GROUP BY filename, filesize, filetime ORDER BY Qty DESC LIMIT 25")

#myLen = len (c.fetchall())
#print (myLen)
for row in c:
    print (row['filename'])
   print ('Qty: ', row['Qty'])
    uprint ("{:,.0f}".format(row['filesize']).replace(",", " ")) 
    print ()

dbConnection.close()
end()
'''

# uncomment to update one table based on 'other' table info
'''
c = dbConnection.execute('SELECT filename, filepath, filesize, filetime FROM newfiles WHERE disk = "750gn"')
filesupdated = 0
for row in c:
    c_new = dbConnection.execute('UPDATE newfiles SET deletion = "notdeleted" WHERE disk = "750g" AND filename = ? AND filepath = ? AND filesize = ? AND filetime = ?', (str(row['filename']),str(row['filepath']),str(row['filesize']),str(row['filetime'])))
    filesupdated += 1
dbConnection.commit()
print ('Files updated: ', "{:,.0f}".format(filesupdated).replace(",", " "))

dbConnection.close()
end()
'''

# get some info on schema, not much usefull, using DB browser better
'''
cursor = dbConnection.cursor()
meta = cursor.execute("PRAGMA table_info('newfiles')")
for r in meta:
    print (r)

cursor = dbConnection.cursor()
cursor.execute("SELECT * FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())
'''


