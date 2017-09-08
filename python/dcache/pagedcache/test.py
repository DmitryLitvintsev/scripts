"""
  "WebDAV-example-1@webdavDomain": {
    "update-time": 5000,
    "protocol": {
      "engine": "org.dcache.webdav.DcacheResourceFactory",
      "root": "/pnfs/usr",
      "family": "https",
      "version": "1.1"
    },
    "interfaces": {
      "131.225.13.253": {
        "url-name": "example.com",
        "address": "131.225.13.253",
        "address-type": "IPv4",
        "FQDN": "example.com",
        "scope": "global"
      }
    },
    "load": 0.0,
    "port": 2880,
    "domain": "webdavDomain",
    "cell": "WebDAV-example-1",
    "tags": {
      "glue": {},
      "srm": {}
    }
  },
"""

import copy
import os
import uuid
import signal
import subprocess
import threading

def createFile(name, size):
    f = open("/tmp/"+name,"wb")
    f.seek(size - 1)
    f.write("\0")
    f.close()

class Test(object):
    def __init__(self,name,dictionary):
        self.dict=copy.deepcopy(dictionary)
        family = protocol = dictionary.get("protocol").get("family")
        self.ro = False
        self.name = name
        self.path = "/pnfs/fnal.gov/usr/eagle/dcache-tests/scratch"
        self.input_filename = "pd_"+family+"_"+str(uuid.uuid4())
        self.output_filename = "pd"+str(uuid.uuid4())
        for key, value in dictionary.get("interfaces").iteritems():
            self.fqdn = value.get("FQDN")

        self.error, self.output = "",""

        self.port = dictionary.get("port")
        self.write_test, self.read_test, self.remove_test = None,None, None

    def __repr__(self):
        return self.name

    def kill_process(self,p):
        p.kill()
        self.error = "TIMEDOUT"


    def run(self):
        fail=0
        try:
            createFile(self.input_filename,104857600)
            for t in (self.write_test,self.read_test):
                if not t:
                    continue
                p = subprocess.Popen(t,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
                timer = threading.Timer(120, self.kill_process,[p])
                try:
                    timer.start()
                    output, error = p.communicate()
                    if output:
                        self.output += output
                    if error:
                        self.error += error
                finally:
                    timer.cancel()
                rc=p.returncode
                if rc != 0 :
                    rc = 1
                    fail |= rc
                    return fail
        except Exception, e:
            fail = 1
            self.error += str(e)
            pass
        finally:
            """
            remove local and remote files vis FS interface
            """

            try:
                os.unlink(os.path.join("/tmp",self.input_filename))
            except:
                pass

            try:
                os.unlink(os.path.join("/tmp",self.output_filename))
            except:
                pass

            if not self.ro:
                try:
                    os.unlink(os.path.join(self.path,self.input_filename))
                except:
                    pass

        return fail


class GsiFtp(Test):
    def __init__(self,name,dictionary):
        Test.__init__(self,name,dictionary)
        self.subpath = "dcache-tests/scratch"

        self.write_test = "globus-url-copy -p 2 file:////tmp/%s gsiftp://%s:%s/%s/%s"%(self.input_filename,
                                                                                       self.fqdn,
                                                                                       self.port,
                                                                                       self.subpath,
                                                                                       self.input_filename)

        self.read_test = "globus-url-copy -p 2 gsiftp://%s:%s/%s/%s file:////tmp/%s"%(self.fqdn,
                                                                                      self.port,
                                                                                      self.subpath,
                                                                                      self.input_filename,
                                                                                      self.output_filename)

#        self.remove_test = "uberftp %s -P %s 'rm %s/%s'"%(self.fqdn,
#                                                          self.port,
#                                                          self.path,
#                                                          self.input_filename)
#

class KerberosFtp(Test):
    def __init__(self,name,dictionary):
        Test.__init__(self,name,dictionary)
        self.subpath = "dcache-tests/scratch"

        self.write_test = "echo 'pagedcache\nput /tmp/%s %s/%s' | timeout 60 /usr/krb5/bin/ftp %s %s"%(self.input_filename,
                                                                                            self.subpath,
                                                                                            self.input_filename,
                                                                                            self.fqdn,
                                                                                            self.port)

        self.read_test = "echo 'pagedcache\nget %s/%s /tmp/%s'| timeout 60 /usr/krb5/bin/ftp %s %s"%(self.subpath,
                                                                                          self.input_filename,
                                                                                          self.output_filename,
                                                                                          self.fqdn,
                                                                                          self.port)

#        self.remove_test = "echo 'pagedcache\ndel %s/%s ' | /usr/krb5/bin/ftp %s %s"%(self.path,
#                                                                                      self.input_filename,
#                                                                                      self.fqdn,
#                                                                                      self.port)

from ftplib import FTP
from ftplib import all_errors

class WeakFtp(Test):
    def __init__(self,name,dictionary):
        Test.__init__(self,name,dictionary)
        self.input_filename = "READTESTFILE"
        self.output_filename = self.input_filename+"_"+str(uuid.uuid4())
        self.subpath = "dcache-tests/scratch"

    def run(self):
        output_file_name = os.path.join("/tmp",self.output_filename)
        try:
            ftp = FTP()
            ftp.connect(self.fqdn,self.port)
            ftp.login("enstore-test", "anon-read")
            self.output=ftp.retrbinary("RETR " + os.path.join(self.subpath,self.input_filename),
                                       open(output_file_name, "wb").write)
            ftp.quit()
        except all_errors,e:
            self.error = str(e)
            return 1
        finally:
            try:
                os.unlink(output_file_name)
            except:
                pass
        return 0

class PlainDcap(Test):
    def __init__(self,name,dictionary):
        Test.__init__(self,name,dictionary)
        self.ro = True
        self.input_filename = "READTESTFILE"
        self.output_filename = self.input_filename+"_"+str(uuid.uuid4())
        self.read_test = "dccp dcap://%s:%s/%s /tmp/%s"%(self.fqdn,
                                                         self.port,
                                                         os.path.join(self.path,self.input_filename),
                                                         self.output_filename,)
    def run(self):
        try :
            del os.environ["DCACHE_IO_TUNNEL"]
        except:
            pass
        return super(PlainDcap,self).run()

class KerberosDcap(Test):
    def __init__(self,name,dictionary):
        Test.__init__(self,name,dictionary)
        self.write_test = "dccp /tmp/%s dcap://%s:%s/%s"%(self.input_filename,
                                                          self.fqdn,
                                                          self.port,
                                                          os.path.join(self.path,self.input_filename),)

        self.read_test = "dccp dcap://%s:%s/%s /tmp/%s"%(self.fqdn,
                                                         self.port,
                                                         os.path.join(self.path,self.input_filename),
                                                         self.output_filename,)

    def run(self):
        os.environ["DCACHE_IO_TUNNEL"] = "/usr/lib64/dcap/libgssTunnel.so"
        return super(KerberosDcap,self).run()

class GsiDcap(Test):
    def __init__(self,name,dictionary):
        Test.__init__(self,name,dictionary)

        self.write_test = "dccp /tmp/%s dcap://%s:%s/%s"%(self.input_filename,
                                                          self.fqdn,
                                                          self.port,
                                                          os.path.join(self.path,self.input_filename),)

        self.read_test = "dccp dcap://%s:%s/%s /tmp/%s"%(self.fqdn,
                                                         self.port,
                                                         os.path.join(self.path,self.input_filename),
                                                         self.output_filename,)

    def run(self):
        os.environ["DCACHE_IO_TUNNEL"] = "/usr/lib64/dcap/libgsiTunnel.so"
        return super(GsiDcap,self).run()


class Https(Test):
    def __init__(self,name,dictionary):
        Test.__init__(self,name,dictionary)
        root = dictionary.get("protocol").get("root")

        self.subpath=self.path[len(root)+1:]

        self.write_test = """
        curl --cert ${X509_USER_PROXY}  --key ${X509_USER_PROXY} \
        --cacert ${X509_USER_PROXY} --capath ${X509_CERT_DIR} \
        -L -T /tmp/%s  https://%s:%s/%s
        """%(self.input_filename,
             self.fqdn,
             self.port,
             os.path.join(self.subpath,self.input_filename),)

        self.read_test = """
        curl --cert ${X509_USER_PROXY}  --key ${X509_USER_PROXY} \
        --cacert ${X509_USER_PROXY} --capath ${X509_CERT_DIR} \
        -L https://%s:%s/%s -o /tmp/%s"""%(self.fqdn,
                                           self.port,
                                           os.path.join(self.subpath,self.input_filename),
                                           self.output_filename)



class Http(Test):
    def __init__(self,name,dictionary):
        Test.__init__(self,name,dictionary)

        self.ro = True
        self.input_filename = "READTESTFILE"
        self.output_filename = self.input_filename+"_"+str(uuid.uuid4())

        self.read_test = """
        curl -L http://%s:%s/%s -o /tmp/%s
        """%(self.fqdn,
             self.port,
             os.path.join(self.path,self.input_filename),
             self.output_filename)


class GsiXrootd(Test):
    def __init__(self,name,dictionary):
        Test.__init__(self,name,dictionary)
        self.write_test = """
        xrdcp /tmp/%s  root://%s:%s/%s
        """%(self.input_filename,
             self.fqdn,
             self.port,
             os.path.join(self.path,self.input_filename),)

        self.read_test = """
        xrdcp root://%s:%s/%s /tmp/%s
        """%(self.fqdn,
             self.port,
             os.path.join(self.path,self.input_filename),
             self.output_filename)

#        self.remove_test = """
#        xrdfs root://%s:%s rm %s
#        """%(self.fqdn,
#             self.port,
#             os.path.join(self.path,self.input_filename))

class PlainXrootd(Test):
    def __init__(self,name,dictionary):
        Test.__init__(self,name,dictionary)
        self.ro = True
        self.input_filename = "READTESTFILE"
        self.output_filename = self.input_filename+"_"+str(uuid.uuid4())
        self.read_test = """
        xrdcp root://%s:%s/%s /tmp/%s
        """%(self.fqdn,
             self.port,
             os.path.join(self.path,self.input_filename),
             self.output_filename)


class Srm(Test):
    def __init__(self,name,dictionary):
        Test.__init__(self,name,dictionary)

        self.write_test = "srmcp  file:////tmp/%s srm://%s:%s/%s/%s"%(self.input_filename,
                                                                      self.fqdn,
                                                                      self.port,
                                                                      self.path,
                                                                      self.input_filename)

        self.read_test = "srmcp srm://%s:%s/%s/%s file:////tmp/%s"%(self.fqdn,
                                                                    self.port,
                                                                    self.path,
                                                                    self.input_filename,
                                                                    self.output_filename)

#        self.remove_test = "srmrm srm://%s:%s/%s/%s"%(self.fqdn,
#                                                      self.port,
#                                                      self.path,
#                                                      self.input_filename)

import shutil

class Nfs(Test):
    def __init__(self,name,dictionary):
        Test.__init__(self,name,dictionary)


    def run(self):

        try:
            createFile(self.input_filename,104857600)

            shutil.copyfile(os.path.join("/tmp",self.input_filename),
                            os.path.join(self.path,self.input_filename))

            shutil.copyfile(os.path.join(self.path,self.input_filename),
                            os.path.join("/tmp",self.output_filename))

        except Exception, e:
            self.error = str(e)
            return 1
        finally:

            try:
                os.unlink(os.path.join("/tmp",self.output_filename))
            except:
                pass

            try:
                os.unlink(os.path.join("/tmp",self.input_filename))
            except:
                pass

            try:
                os.unlink(os.path.join(self.path,self.input_filename))
            except:
                pass
        return 0




def createTest(name,dictionary):
    protocol = dictionary.get("protocol")
    if not protocol:
        return None
    family   = protocol.get("family")
    if family == "gsiftp":
        return GsiFtp(name,dictionary)
    elif family == "gkftp":
        return KerberosFtp(name,dictionary)
    elif family == "ftp":
        return WeakFtp(name,dictionary)
    elif family == "dcap":
        if name.find("kerberosdcap") != -1:
            return KerberosDcap(name,dictionary)
        else:
            return PlainDcap(name,dictionary)
    elif family == "gsidcap":
        return GsiDcap(name,dictionary)
    elif family == "http":
        return Http(name,dictionary)
    elif family == "https":
        return Https(name,dictionary)
    elif family == "file":
        return Nfs(name,dictionary)
    elif family == "srm":
        return Srm(name,dictionary)
    elif family == "root":
        port = dictionary.get("port")
        if port == 1094:
            return GsiXrootd(name,dictionary)
        else:
            return PlainXrootd(name,dictionary)
    else:
        print "Protocol %s is not supported"%(family,)
        return None


