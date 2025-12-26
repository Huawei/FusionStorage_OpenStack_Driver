#!/bin/bash

sudo /usr/bin/ln -s /host/sbin/multipath /sbin/multipath
sudo /usr/bin/ln -s /host/sbin/multipathd /sbin/multipathd
sudo /usr/bin/ln -s /host/usr/lib64/libmultipath.so.0 /usr/lib64/libmultipath.so.0
sudo /usr/bin/ln -s /host/usr/lib64/libmpathpersist.so.0 /usr/lib64/libmpathpersist.so.0
sudo /usr/bin/ln -s /host/usr/lib64/libmpathcmd.so.0 /usr/lib64/libmpathcmd.so.0

sudo /usr/bin/cp -R /block_driver_dir/fusionstorage /usr/lib/python2.7/site-packages/cinder/volume/drivers
sudo /usr/bin/cp -R /block_driver_dir/san /usr/lib/python2.7/site-packages/cinder/volume/drivers
