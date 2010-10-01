#Copyright (c) 2010, treeform
#All rights reserved.

#Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

#Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
#Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials #provided with the distribution.
#Neither the name of the treeform nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written #permission.
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF #MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, #SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS #INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE #USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
from fabulous.color import *

header = """\
                    _         
 _ __ _  _ ___ __ _| |___ _ _ 
| '  \ || (_-</ _` | / -_) '_|
|_|_|_\_, /__/\__, |_\___|_|  
      |__/     by|_|treeform          
"""

print header
help = """
    q - quit 
    ? - help
    h - show histroy
    sql :: filter row by string
    sql << procces each row with python (e = entry, ed = entry as dict)
    << run python just for kicks (last = full data set of entry dicts)
    sql >> write to file.csv (using ,)
    sql \ dont cut the data at terminal height
"""

import os,sys
import _mysql
import gridprint
import re
import os
import readline
import atexit


def terminal_width():
    """Return estimated terminal width."""
    width = 0
    try:
        import struct, fcntl, termios
        s = struct.pack('HHHH', 0, 0, 0, 0)
        x = fcntl.ioctl(1, termios.TIOCGWINSZ, s)
        width = struct.unpack('HHHH', x)[1]
    except IOError:
        pass
    if width <= 0:
        try:
            width = int(os.environ['COLUMNS'])
        except:
            pass
    if width <= 0:
        width = 80

    return width

def terminal_height():
    """Return estimated terminal width."""
    width = 0
    try:
        import struct, fcntl, termios
        s = struct.pack('HHHH', 0, 0, 0, 0)
        x = fcntl.ioctl(1, termios.TIOCGWINSZ, s)
        width = struct.unpack('HHHH', x)[0]
    except IOError:
        pass
    if width <= 0:
        try:
            width = int(os.environ['COLUMNS'])
        except:
            pass
    if width <= 0:
        width = 80

    return width


def qscape(s):
    return repr(s)[1:-1]

def cvsq(s):
    if "," in s or "\n" in s:
        return repr(s)
    return s


class History:

    def __init__(self):
        histfile = os.path.join(os.environ["HOME"], ".mysqler")
        try:
            readline.read_history_file(histfile)
        except IOError:
            pass
        atexit.register(readline.write_history_file, histfile)

    def get_all(self):
        histfile = os.path.join(os.environ["HOME"], ".mysqler")
        return open(histfile).read()    



class DBCompleter(object):
    
    def __init__(self,dbm):
        self.options = ["select","from","like","where","update",
            "describe","show","tables","count","distinct"]
        self.tables = {}
        readline.parse_and_bind('tab: complete')
        readline.set_completer(self.complete)
        self.register(dbm)

    def register(self,dbm):
        h,data = dbm.sql("show tables")
        for entry in data:          
            table = entry[0]
            self.tables[table] = None
            self.options.append(table)
            self.options.append("%s.%s"%(dbm.database,table))

    def complete(self, text, state):
        response = None
        options = list(self.options)

        buffer = readline.get_line_buffer()
        for table in self.tables.keys():
            if table in buffer:
                fields = self.tables[table]
                if fields == None:
                    fields = [field[0] for field in dbm.sql("describe %s"%table)[1]]
                    self.tables[table] = fields
                options = options + fields

        if state == 0:
            #print 
            #print repr(text),len(options)
            if text:
                self.matches = [s for s in options
                    if s.startswith(text)]
            else:
                self.matches = options[:]
        
        try:
            response = self.matches[state]
        except IndexError:
            response = None
        return response


class DBManager:

    def __init__(self,connect=None):

        try:
            if not connect:
                connect = sys.argv[1]
            match = re.match("([^@]*)@([^:]*):([^/]*)/(.*)",connect)
        except:
            print "connection string in this format required:"
            print "    username@hostname:database/password"
            sys.exit()

        self.user = match.group(1)
        self.host = match.group(2)
        self.database = match.group(3)
        self.passwd = match.group(4)
        self.reconnect()

    def reconnect(self):
        self.db = _mysql.connect(self.host,self.user,self.passwd,self.database)


    def sql(self,q):
        try:
            self.db.query(q)
        except Exception,e:
            if "(2006, 'MySQL server has gone away')" == str(e):
                print red("reconecting...")
                self.reconnect()
                self.db.query(q)
            else:
                print red("E:"+str(e))

        r = self.db.store_result()
        if r:
            data = r.fetch_row(0,how=0)
            headers = [h[0] for h in  r.describe()]
            return headers, data
        return ([],[])

    def sqldo(self, q):
        db.query(q)



class CommandInterface:


    def __init__(self):

        last = None
        lastcommand = None

        while True:

            try:
                command = raw_input("::")
            except KeyboardInterrupt:
                print
                sys.exit(0)

            if command == "":
                print "  starting multi line end text with ';' go "
                while ";" not in command:
                     command += " " + raw_input(">>")

            if command == "q": break
            if command == "?": 
                print help
                print "connectred to:",dbm.user+"@"+dbm.host+":"+dbm.database+"/"+dbm.passwd
                continue
            

            search = None
            if "::" in command:
                _command = command[0:command.find("::")]
                search = command[command.find("::")+2:].strip()
                print "  filtering by ",repr(search)
                command = _command

            python = None
            if "<<" in command:
                _command = command[0:command.find("<<")]
                python = command[command.find("<<")+2:].strip()
                command = _command
                if not command:
                    try:
                        exec python
                    except Exception,e:
                        print e
                    continue
                print "  per entry", python

            file = None
            if ">>" in command:
                _command = command[0:command.find(">>")]
                filename = command[command.find(">>")+2:].strip()
                if filename == '':
                    file = sys.stdout
                else:
                    file = open(filename,'w')
                command = _command

            if not command and lastcommand:
                command = lastcommand
                
            if command in compleater.tables:
                command = "select * from %s limit %i"%(
                    command,terminal_height()-4)
                print command

            if command and "\\" != command[-1]:
                cutAt = terminal_height()-4
            else:
                cutAt = 1000000
                command = command[:-1]
                print command
                
            if "h" == command:
                print history.get_all()
                continue

            cut = False
            first = True
            h,data = dbm.sql(command)
            last = data

            d = []
            if file:
                print >>file, ",".join(h)
            else:
                d.append(h)

            if h:
                for row in data:
                    row = map(qscape,row)

                    if len(d) > cutAt: 
                        cut = True
                        break

                    if python : 
                        print "==="*20,row,eval(python,locals())                           
                    
                        d.append(eval(python,locals()))   

                    elif not search or search in "".join(row): 
                        if file:
                            print >>file, ",".join(cvsq(row))
                        else:
                            d.append(row)   

                gridprint.display(d,width=terminal_width())        
            
            if file and file != sys.stdout: file.close()

            if cut:
                print yellow("...top %i out of %i ([SQL]\ to show)..."%(len(d)-1,len(data)))

        
        lastcommand = command


history = History()
dbm = DBManager()
compleater = DBCompleter(dbm)
command = CommandInterface()
