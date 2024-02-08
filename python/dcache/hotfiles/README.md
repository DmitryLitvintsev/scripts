pip install --upgrade pip
yum install krb5-devel-1.15.1-50.el7.x86_64

yum install krb5-devel
curl -O https://bootstrap.pypa.io/pip/2.7/get-pip.py
python get-pip.py
python -m pip install --upgrade "pip < 21.0"
yum install --disablerepo=epel python-devel
pip install "paramiko[gssapi]"
pip install pandas
pip install tabulate
pip uninstall psycopg2
pip install DBUtils==1.3
pip install python-gssapi


yum install --disableexcludes=all  postgresql11*
