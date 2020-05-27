
# ImpalaReader 插件文档

___


## 1 快速介绍

ImpalaReader插件实现了从Impala读取数据。在底层实现上，ImpalaReader通过JDBC连接远程Impala数据库，并执行相应的sql语句将数据从Impala库中SELECT出来。

## 2 实现原理

简而言之，ImpalaReader通过JDBC连接器连接到远程的Impala数据库，并根据用户配置的信息生成查询SELECT SQL语句并发送到远程Impala数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，ImpalaReader将其拼接为SQL语句发送到Impala数据库；对于用户配置querySql信息，Impala直接将其发送到Impala数据库。


## 3 功能说明

### 3.1 配置样例

* 配置一个从Impala数据库同步抽取数据到本地的作业:

```
{
    "job": {
        "setting": {
            "speed": {
                 "byte": 1048576
            }
        },
        "content": [
            {
                "reader": {
                    "name": "Impalareader",
                    "parameter": { 
                        "datasource": {
                            "initialSize": 1,
                            "minIdle": 1,
                            "maxActive": 3,
                            "maxWait": 60000,
                            "timeBetweenEvictionRunsMillis": 3000,
                            "minEvictableIdleTimeMillis": 360000,
                            "validationQuery": "SELECT 1",
                            "testWhileIdle": true,
                            "testOnBorrow": true,
                            "testOnReturn": true,
                            "poolPreparedStatements": false,
                            "maxPoolPreparedStatementPerConnectionSize": 0,
                            "useGlobalDataSourceStat": true,
                            "removeAbandoned": true,
                            "removeAbandonedTimeout": 600,
                            "logAbandoned": false
                        },
                        "column": [
                            "id"
                        ],
                        "fetchSize":5000,
                        "connection": [
                            {
                                "table": [
                                    "table"
                                ],
                                "jdbcUrl": "jdbc:impala://192.168.1.1:21050;AuthMech=0;SSL=0"
                            }
                        ]
                    }
                },
               "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": true,
                        "encoding": "UTF-8"
                    }
                }
            }
        ]
    }
}
```

* 配置一个自定义SQL的数据库同步任务到本地内容的作业：

```
{
    "job": {
        "setting": {
            "speed": 1048576
        },
        "content": [
            {
                {
                    "name": "impalareader",
                    "parameter": {
                        "datasource": {
                            "initialSize": 1,
                            "minIdle": 1,
                            "maxActive": 3,
                            "maxWait": 60000,
                            "timeBetweenEvictionRunsMillis": 3000,
                            "minEvictableIdleTimeMillis": 360000,
                            "validationQuery": "SELECT 1",
                            "testWhileIdle": true,
                            "testOnBorrow": true,
                            "testOnReturn": true,
                            "poolPreparedStatements": false,
                            "maxPoolPreparedStatementPerConnectionSize": 0,
                            "useGlobalDataSourceStat": true,
                            "removeAbandoned": true,
                            "removeAbandonedTimeout": 600,
                            "logAbandoned": false
                        }, 
                        "connection": [
                            { 
                                "jdbcUrl": "",
                                "querySql": "SELECT * from test"
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "visible": false,
                        "encoding": "UTF-8"
                    }
                }
            }
        ]
    }
}
```


### 3.2 参数说明
* **datasource**
	* 描述：描述的是druid连接池连接impala的配置信息，使用JSON的字符串描述。	

	* 必选：是 <br />

	* 默认值：无 <br />

* **jdbcUrl**

	* 描述：描述的是到对端数据库的JDBC连接信息，使用JSON的字符串描述。	

	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：数据源的用户名 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **password**

	* 描述：数据源指定用户名的密码 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **table**

	* 描述：所选取的需要同步的表，使用JSON的数组描述(当前仅支持单张表同步)。br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **column**

	* 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。用户使用\*代表默认使用所有列配置，例如["\*"]。

	  支持常量配置，用户需要按照JSON格式:
	  ["id", "[table]", "1", "'bazhen.csy'", "null", "COUNT(*)", "2.3" , "true"]
	  id为普通列名，[table]为包含保留在的列名，1为整形数字常量，'bazhen.csy'为字符串常量，null为空指针，to_char(a + 1)为表达式，2.3为浮点数，true为布尔值。

		column必须用户显示指定同步的列集合，不允许为空！

	* 必选：否 <br />

	* 默认值：无 <br />

* **splitPk**
    * 此参数在此不支持

* **where**

	* 描述：筛选条件，MysqlReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。<br />

          where条件可以有效地进行业务增量同步。如果该值为空，代表同步全表所有的信息。

	* 必选：否 <br />

	* 默认值：无 <br />

* **querySql**

	* 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。<br />

	 `当用户配置querySql时，ImpalaReader直接忽略table、column、where条件的配置`。

	* 必选：否 <br />

	* 默认值：无 <br />

* **fetchSize**

	* 描述：该配置项定义了插件和数据库服务器端每次批量数据获取条数，该值决定了DataX和服务器端的网络交互次数，能够较大的提升数据抽取性能。<br />

	 `注意，该值过大可能造成DataX进程OOM，可适当调整Datax内存设置`。

	* 必选：否 <br />

	* 默认值：5000 <br />

