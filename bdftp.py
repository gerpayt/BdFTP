# coding:utf8
import socket, threading, os, sys, time
import hashlib, platform, stat
import urllib2
import StringIO
import shlex
# convert python's encoding to utf8
reload(sys)
sys.setdefaultencoding('utf8')

listen_ip = "0.0.0.0"
listen_port = 21
conn_list = []
max_connections = 500
conn_timeout = 120

from pybaidudisk import NetDisk

class FtpConnection(threading.Thread):
    def __init__(self, fd):
        threading.Thread.__init__(self)
        self.fd = fd
        self.running = True
        self.setDaemon(True)
        self.alive_time = time.time()
        self.option_utf8 = False
        self.identified = False
        self.option_pasv = True
        self.username = ""
        self.prefix = "/"
        self.pwd = "/"
        self.startpoint = 0
        self.client = None
    def process(self, cmd, arg):
        cmd = cmd.upper();
        if self.option_utf8:
            arg = unicode(arg, "utf8").encode(sys.getfilesystemencoding())
        print "<<", cmd, arg, self.fd
        # Ftp Command
        if cmd == "BYE" or cmd == "QUIT":
            self.message(221, "Bye!")
            self.running = False
            return
        elif cmd == "USER":
            # Set Anonymous User
            if arg == "": arg = "anonymous"
            self.username = arg.replace('#','@')
            #if not os.path.isdir(self.home_dir):
            #    self.message(530, "User " + self.username + " not exists.")
            #    return
            #self.pass_path = self.home_dir + "/.xxftp/password"
            #if os.path.isfile(self.pass_path):
            self.message(331, "Password required for " + self.username)
            #else:
            #self.message(230, "Identified!")
            #self.identified = True
            return
        elif cmd == "PASS":
            self.client = NetDisk(self.username,arg)
            if self.client.check_login():
                self.message(230, "Identified!")
                self.identified = True
            else:
                self.message(530, "Not identified!")
                self.identified = False
            return
        elif not self.identified:
            self.message(530, "Please login with USER and PASS.")
            return

        self.alive_time = time.time()
        finish = True
        if cmd == "NOOP":
            self.message(200, "ok")
        elif cmd == "TYPE":
            self.message(200, "ok")
        elif cmd == "SYST":
            self.message(200, "UNIX")
        elif cmd == "EPSV" or cmd == "PASV":
            #self.message(500, "failed to create data socket.")
            self.option_pasv = True
            try:
                self.data_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.data_fd.bind((listen_ip, 0))
                self.data_fd.listen(1024)
                ip, port = self.data_fd.getsockname()
                if cmd == "EPSV":
                    self.message(229, "Entering Extended Passive Mode (|||" + str(port) + "|)")
                else:
                    ipnum = socket.inet_aton(ip)
                    self.message(227, "Entering Passive Mode (%s,%u,%u)" %
                        (",".join(ip.split(".")), (port>>8&0xff), (port&0xff)))
            except:
                self.message(500, "failed to create data socket.")
        elif cmd == "EPRT":
            self.message(500, "implement EPRT later...")
        elif cmd == "PORT":
            self.option_pasv = False
            self.data_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s = arg.split(",")
            self.data_ip = ".".join(s[:4])
            self.data_port = int(s[4])*256 + int(s[5])
            self.message(200, "ok")
        elif cmd == "PWD" or cmd == "XPWD":
            self.message(257, '"' + self.pwd + '"')
        elif cmd == "LIST" or cmd == "NLST":
            #if arg != "" and arg[0] == "-": arg = "" # omit parameters

            if not self.establish(): return
            self.message(150, "ok")

            try:
                path, = shlex.split(arg)
                path = os.path.normpath(os.path.join(self.pwd, path))
            except:
                path = self.pwd
            #print 'list path', path
            files = self.client.list(path)

            #print "total", len(files), path
            for f in files:
                info = "%srwxrwxrwx %04u %8s %8s %8lu %s %s\r\n" % (
                        "d" if f['isdir'] else "-" , 1, "0", "0", f.get('size', 0),
                        time.strftime("%b %d %Y", time.localtime(f['server_mtime'])),
                        f['server_filename'].encode("utf8"))
                #print info.strip()
                self.data_fd.send(info)
            self.message(226, "Directory send OK." )
                #info = "%srwxrwxrwx %04u %8s %8s %8lu %s %s\r\n" % (
                #        "d" if f['isdir'] else "-" , 1, "0", "0", file['fsize'],
                #        time.strftime("%b %d %Y", time.localtime(file['putTime']/10000000)),
                #        f['server_filename'].encode("utf8"))

            #else:
            #    print "no such dir"
            #    self.message(550, "failed.")

            self.data_fd.close()
            self.data_fd = 0
        elif cmd == "REST":
            self.startpoint = int(arg)
            self.message(250, "ok")
        elif cmd == "FEAT":
            features = "211-Features:\r\nSITES\r\nEPRT\r\nEPSV\r\nMDTM\r\nPASV\r\n"\
                "REST STREAM\r\nSIZE\r\nUTF8\r\n211 End\r\n"
            self.fd.send(features)
        elif cmd == "OPTS":
            arg = arg.upper()
            if arg == "UTF8 ON":
                self.option_utf8 = True
                self.message(200, "ok")
            elif arg == "UTF8 OFF":
                self.option_utf8 = False
                self.message(200, "ok")
            else:
                self.message(500, "unrecognized option")
        elif cmd == "CDUP":
            #print 'pwd',self.pwd
            new_pwd = self.pwd[:self.pwd.rstrip('/').rfind('/')+1]
            #print new_pwd
            if self.client.list(new_pwd.decode('utf-8')):
                self.pwd = new_pwd
                self.message(250, '"' + self.pwd + '"')
            else:
                self.message(550, "failed.")
        else:
            finish = False
        if finish: return
        # Parse argument ( It's a path )
        if arg == "":
            self.message(500, "where's my argument?")
            return
        #remote, local = self.parse_path(arg)
        # can not do anything to virtual directory
        #newpath = local
        #try:
        if cmd == "CWD":
            if arg == '':
                self.pwd = '/'
            else:
                p = os.path.normpath(os.path.join(self.pwd, arg))
                self.pwd = p
            #print 'cwd', self.pwd
            #info = self.client.isdir(self.pwd+"/")
            #print info
            #if path:
            self.message(250, '"' + self.pwd + '"')
            #else:
            #    self.message(550, "failed.")
        elif cmd == "MDTM":
            p = os.path.normpath(os.path.join(self.pwd, arg))
            filename = os.path.basename(p)
            for f in self.client._list(os.path.dirname(p)):
                if f['server_filename'] == filename:
                    #print f
                    self.message(213, time.strftime("%Y%m%d%I%M%S", time.localtime(f['server_mtime'])))
                    return
            self.message(213, time.strftime("%Y%m%d%I%M%S", time.localtime(0)))
        elif cmd == "SIZE":
            p = os.path.normpath(os.path.join(self.pwd, arg))
            filename = os.path.basename(p)
            for f in self.client._list(os.path.dirname(p)):
                if f['server_filename'] == filename:
                    #print f
                    self.message(231, f['size'])
                    return
            self.message(231, 0)

        elif cmd == "XMKD" or cmd == "MKD":
            p = os.path.normpath(os.path.join(self.pwd, arg))
            ret = self.client.mkdir(p)
            if ret['errno'] == 0:
                #print ret['path'], "save ok!"
                self.message(250, "ok")
            else:
                #print 'error', ret
                self.message(550, "failed.")
        elif cmd == "RNFR":
            if arg.startswith('/'):
                self.temp_path = arg.rstrip('/')
            else:
                self.temp_path = os.path.normpath(os.path.join(self.pwd, arg))
            self.message(350, "rename from " + self.temp_path)
        elif cmd == "RNTO":
            src = self.temp_path
            if arg.startswith('/'):
                dst = arg.rstrip('/')
            else:
                dst = os.path.normpath(os.path.join(self.pwd, arg))

            fname = os.path.basename(src)

            if self.client.isdir(dst):
                ret = self.client.move(src, dst, fname)
            else:
                ret = self.client.move(src, os.path.dirname(dst), os.path.basename(dst))

            if ret['errno'] == 0:
                for i in ret['info']:
                    #print i['path'], i['errno']
                self.message(250, "RNTO to " + dst)
            else:
                #print "error:", ret
                self.message(550, 'error.')

        elif cmd == "XRMD" or cmd == "RMD" or cmd == "DELE":
            path = os.path.normpath(os.path.join(self.pwd, arg))
            ret = self.client.remove(path)
            if ret['errno'] == 0:
                for i in ret['info']:
                    print i['path'], i['errno']
                self.message(250, "ok")
            else:
                print "error:", ret
                self.message(550, "error.")

        elif cmd == "RETR":
            p = os.path.normpath(os.path.join(self.pwd, arg))
            filename = os.path.basename(p)
            for f in self.client._list(os.path.dirname(p)):
                #print f
                if f['server_filename'] == filename:
                    if f.has_key('thumbs') and f['thumbs'].has_key('url3'):
                        if not self.establish(): return
                        self.message(150, "ok")
                        #print f
                        url = f['thumbs']['url3']
                        #url = f['dlink']
                        private_url = url
                        #print private_url
                        request = urllib2.Request(private_url)
                        #print "Range", "bytes=%d-%d" % (self.startpoint, f['size'])
                        request.add_header("Range", "bytes=%d-%d" % (self.startpoint, f['size']))
                        response = urllib2.urlopen(request)
                        while self.running:
                            self.alive_time = time.time()
                            #data = response.read(8192)
                            #data = response.read(1024*1024)
                            data = response.read(4*1024)
                            # TODO ?
                            if len(data) == 0: break
                            self.data_fd.send(data)
                        response.close()
                        self.startpoint = 0
                        self.data_fd.close()
                        self.data_fd = 0
                        self.message(226, "ok")
                    else:
                        self.message(550, u"由于国家政策限制，暂时不能下载本文件。")
        elif cmd == "STOR" or cmd == "APPE":
            if not self.establish(): return
            self.message(150, "ok")
            #path = os.path.normpath(os.path.join(self.pwd, arg))
            temp_path = 'temp/'+arg
            f = open(temp_path, ("ab" if cmd == "APPE" else "wb") )
            while self.running:
                self.alive_time = time.time()
                data = self.data_fd.recv(16*1024)
                if len(data) == 0: break
                f.write(data)
            f.close()
            self.message(250, "ok")
            #print 'file_recived!'
            self.data_fd.close()
            self.data_fd = 0
            if os.path.isfile(temp_path):
                ret = self.client.upload(temp_path, self.pwd)
                if ret['errno'] == 0:
                    #print ret['path'], "save ok!"
                    self.message(226, "ok")
                else:
                    #print 'error', ret
                    self.message(550, "error.")
            else:
                self.message(550, "error.")
            os.unlink(temp_path)
        else:
            self.message(500, cmd + " not implemented")
            self.startpoint = 0
    #except:
    #    self.message(550, "failed.")

    def establish(self):
        if self.data_fd == 0:
            self.message(500, "no data connection")
            return False
        if self.option_pasv:
            fd = self.data_fd.accept()[0]
            self.data_fd.close()
            self.data_fd = fd
        else:
            try:
                self.data_fd.connect((self.data_ip, self.data_port))
            except:
                self.message(500, "failed to establish data connection")
                return False
        return True

    def run(self):
        ''' Connection Process '''
        try:
            if len(conn_list) > max_connections:
                self.message(500, "too many connections!")
                self.fd.close()
                self.running = False
                return
            # Welcome Message
            self.message(220, "BdFTP Welcome!")
            # Command Loop
            line = ""
            while self.running:
                data = self.fd.recv(4096)
                if len(data) == 0: break
                line += data
                if line[-2:] != "\r\n": continue
                line = line[:-2]
                space = line.find(" ")
                if space == -1:
                    self.process(line, "")
                else:
                    self.process(line[:space], line[space+1:])
                line = ""
        except:
            print "error", sys.exc_info()
        self.running = False
        self.fd.close()
        print "connection end", self.fd, "user", self.username

    def message(self, code, s):
        ''' Send Ftp Message '''
        print '>>', code, s
        s = str(s).replace("\r", "")
        ss = s.split("\n")
        if len(ss) > 1:
            r = (str(code) + "-") + ("\r\n" + str(code) + "-").join(ss[:-1])
            r += "\r\n" + str(code) + " " + ss[-1] + "\r\n"
        else:
            r = str(code) + " " + ss[0] + "\r\n"
        if self.option_utf8:
            r = unicode(r, sys.getfilesystemencoding()).encode("utf8")
        self.fd.send(r)

def server_listen():
    global conn_list
    listen_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_fd.bind((listen_ip, listen_port))
    listen_fd.listen(1024)
    conn_lock = threading.Lock()
    print "ftpd is listening on ", listen_ip + ":" + str(listen_port)

    while True:
        conn_fd, remote_addr = listen_fd.accept()
        print "connection from ", remote_addr, "conn_list", len(conn_list)
        conn = FtpConnection(conn_fd)
        conn.start()

        conn_lock.acquire()
        conn_list.append(conn)
        # check timeout
        try:
            curr_time = time.time()
            for conn in conn_list:
                if int(curr_time - conn.alive_time) > conn_timeout:
                    if conn.running == True:
                        conn.fd.shutdown(socket.SHUT_RDWR)
                    conn.running = False
            conn_list = [conn for conn in conn_list if conn.running]
        except:
            print sys.exc_info()
        conn_lock.release()


def main():
    server_listen()
    
if __name__ == "__main__":
    main()


