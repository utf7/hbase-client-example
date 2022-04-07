#  HBase Client Example

hbase client example will show how to use hbase with java api

include ddl and dml 

ddl : list tables,create table ,alter table,read region location,compact,split 

dml: put,batch put,delete,increment,scan,get

## Requirements:

* Windows or Linux or Mac OS X

* Java 8

* Maven 3.6.3+

## Building HBase Client Example

```shell
git clone https://github.com/utf7/hbase-client-example.git
cd hbase-client-example
mvn clean package
```

##  Running the Test

#### import config

`hbase-site.xml` is the config file for hbase client to use

a simple hbase-site.xml client config like 

```xml
<configuration>
    <property>
        <name>hbase.zookeeper.quorum</name>
        <value>zk1,zk2,zk3</value>
    </property>
    <property>
        <name>hbase.zookeeper.property.clientPort</name>
        <value>2181</value>
    </property>
    <property>
        <name>zookeeper.znode.parent</name>
        <value>/hbase</value>
    </property>
</configuration>
  
 ```

####  Run fat-jar


if your  `hbase-site.xml`  in the folder `/home/conf/`  

```shell script
java -cp /home/conf/:hbase-client-example-1.0.jar com.github.utf7.hbase.client.example.HBaseDemo 100 
```

`100` means write `100` rows data to hbase

#### Region Tool

**merge region**

thin jar mode 

```shell script
	 java -cp "original-hbase-client-example.jar:/usr/local/hbase/lib/*:/usr/local/hbase/conf/" com.github.utf7.hbase.tool.RegionTool "default:test_to_merge_tb" 1024
```

#### RegionName

```shell  script
Bytes.toString(regionInfo.getRegionName()) : hb_test,,1647862081929.adfbdd6411c6015c2b93ec0189f7ec8d.
getRegionNameAsString : hb_test,,1647862081929.adfbdd6411c6015c2b93ec0189f7ec8d.
Bytes.toString(regionInfo.getEncodedNameAsBytes()):   adfbdd6411c6015c2b93ec0189f7ec8d
getEncodedName : adfbdd6411c6015c2b93ec0189f7ec8d
getRegionId : 1647862081929
```

#### HBase Shell Create Table example

```
create 'test', {NAME => 'f1', VERSIONS => '1', COMPRESSION => 'GZ', DATA_BLOCK_ENCODING => 'FAST_DIFF',  CONFIGURATION => {'hbase.hstore.compaction.max' => '15','hbase.hstore.compaction.min' => '7','hbase.hregion.max.filesize' => '21474836480', 'hbase.hregion.majorcompaction' => '0'}},{NUMREGIONS => 30,SPLITALGO => 'DecimalStringSplit'}
```
