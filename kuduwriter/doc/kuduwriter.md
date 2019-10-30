# DataX KuduWriter


---


## 1 快速介绍

KuduWriter 插件实现了写入数据到 Kudu 主库的目的表的功能。在底层实现上， KuduWriter 通过 Api 连接远程 Kudu 数据库，并执行相应的 insert或者upsert语句将数据写入 Kudu，内部会分批次提交入库，需要数据库本身进行性能调优。

KuduWriter 面向ETL开发工程师，他们使用 KuduWriter 从数仓导入数据到 Kudu。同时 KuduWriter 亦可以作为数据迁移工具为DBA等用户提供服务。


## 2 实现原理

KuduWriter 通过 DataX 框架获取 Reader 生成的协议数据，根据你配置的 `writeMode` 生成


* `insert`(当主键/唯一性索引冲突时会写不进去冲突的行)

##### 或者

* `upsert`(没有遇到主键/唯一性索引冲突时，与 insert行为一致，冲突时会用新行替换原有行所有字段) 的语句写入数据到 Kudu。

<br />

## 3 功能说明

### 3.1 配置样例

* 这里使用一份从 Mysql 产生到 Kudu 导入的数据。

```json
{
	"job": {
		"setting": {
			"speed": {
				"channel": 1,
                                "byte":134217728
			}
		},
		"content": [{
			"reader": {
				"name": "mysqlreader",
				"parameter": {
					"username": "test",
					"password": "test",
					"connection": [{
						"querySql": [
							"select order_no,sap_order_no,vbak_vkorg,vbak_vtweg,vbap_spart,knvv_vkbur,knvv_vkgrp,order_send_allow,is_order_send,invoice_send_allow,is_invoice_send,logist_sent_allow,is_logist_send,received_sent_allow,is_received_send,account_sent_allow,is_account_send,remark,rowid,rowversion,three_order_no,c_delivery,is_map,curr_type,is_posting from test.order_tab where rowversion >= '2019-01-01 00:00:00'"
						],
						"jdbcUrl": [
							"jdbc:mysql://192.172.1.1:3306/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai"]
					}]
				}
			},
			"writer": {
				"name": "kuduwriter",
				"parameter": {
					"writeMode": "upsert",
					"primaryKey": "order_no",
					"namespace": "test",
					"flushBufferSize": 15000,
					"column": ["order_no","sap_order_no","vbak_vkorg","vbak_vtweg","vbap_spart","knvv_vkbur","knvv_vkgrp","order_send_allow","is_order_send","invoice_send_allow","is_invoice_send","logist_sent_allow","is_logist_send","received_sent_allow","is_received_send","account_sent_allow","is_account_send","remark","rowid","rowversion","three_order_no","c_delivery","is_map","curr_type","is_posting"],
					"preSql": [],
					"connection": [{
						"jdbcUrl": "192.172.1.3:7051,192.172.1.2:7051,192.172.1.4:7051",
						"table": ["test"]
					}]
				}
			}
		}]
	}
}

```


### 3.2 参数说明

* **jdbcUrl**

	* 描述：Kudu的连接地址(多个以逗号分隔)

 	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：暂未用到 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **password**

	* 描述：暂未用到 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **table**

	* 描述：Kudu目的表的表名称(暂只支持一个表)

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔，必须与Reader读出的字段一致。

	* 必选：是 <br />

	* 默认值：否 <br />

* **writeMode**

	* 描述：控制写入数据到目标表采用 `insert` 或者 `upsert` 语句<br />

	* 必选：是 <br />
	
	* 所有选项：insert/upsert <br />

	* 默认值：upsert <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与Kudu的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：100000 <br />


### 3.3 类型转换

类似 KuduReader ，目前 KuduWriter 支持大部分 Kudu 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。
同时，当前存入Kudu的数据类型都是string类型，时间类型字段都转成了时间戳来字符串存储。

下面列出 KuduWriter 针对 Kudu 类型转换列表:


| DataX 内部类型| Kudu 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint, year|
| Double   |float, double, decimal|
| String   |varchar, char, tinytext, text, mediumtext, longtext    |
| Date     |date, datetime, timestamp, time    |
| Boolean  |bit, bool   |
| Bytes    |tinyblob, mediumblob, blob, longblob, varbinary    |

 * `bit类型目前是未定义类型转换`

