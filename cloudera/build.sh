#!/bin/sh

GEARPUMP_HOME=`pwd`

if [ ! -f version.sbt ]; then
   echo "must run this command under Gearpump root dir."
   exit 1
fi

cd $GEARPUMP_HOME/
#build Gearpump
sbt clean assembly pack

#get version
GEARPUMP_VERSION=`cat $GEARPUMP_HOME/version.sbt |cut -d'"' -f2`
echo "Gearpump version is $GEARPUMP_VERSION"

CLOUDERA_TOPDIR=$GEARPUMP_HOME/output/target/cloudera
CLOUDERA_PARCELDIR=$CLOUDERA_TOPDIR/parcel
mkdir -p $CLOUDERA_PARCELDIR
cp -r $GEARPUMP_HOME/output/target/pack $CLOUDERA_PARCELDIR
cp -r $GEARPUMP_HOME/cloudera/parcel $CLOUDERA_PARCELDIR/pack
cd $CLOUDERA_PARCELDIR/pack
mv parcel meta
#replace version string in meta files
cd $CLOUDERA_PARCELDIR/pack/meta/
metalist=`ls .`
for f in $metalist
do
  echo "##################3 replace $f"
  sed -i "s/{version}/$GEARPUMP_VERSION/" $f
done

#package
PACKAGE_NAME=gearpump-${GEARPUMP_VERSION}
cd $CLOUDERA_PARCELDIR
mv pack $PACKAGE_NAME
tar zcvf ${PACKAGE_NAME}-el6.parcel $PACKAGE_NAME --owner=root --group=root
for suffix in el5 sles11 lucid precise squeeze wheezy
do
	ln -s ${PACKAGE_NAME}-el6.parcel ${PACKAGE_NAME}-${suffix}.parcel
done
rm $CLOUDERA_PARCELDIR/$PACKAGE_NAME -rf

#create manifest
cd /tmp
git clone https://github.com/cloudera/cm_ext.git
cd $CLOUDERA_PARCELDIR
python /tmp/cm_ext/make_manifest/make_manifest.py
rm -rf /tmp/cm_ext


cd $GEARPUMP_HOME/cloudera/csd
mvn package
CLOUDERA_CSDDIR=$CLOUDERA_TOPDIR/csd
mkdir -p $CLOUDERA_CSDDIR
cp $GEARPUMP_HOME/cloudera/csd/target/*.jar $CLOUDERA_CSDDIR

echo ""
echo ""
echo ""
echo "#######################################################"
echo "Gearpump parcels are built under $CLOUDERA_PARCELDIR."
echo "Gearpump CSD is under $CLOUDERA_CSDDIR."
echo "You can find installation guide at http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html"
echo "#######################################################"
