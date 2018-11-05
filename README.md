### 这是什么
这是一个经量级的数据传输（导入/导出）服务，支持计划支持多种不同数据库（Oracle/MySQL/PostgreSQL/HBase/Elasticsearch等）之前的数据传输。

### 数据传输配置
每个数据传输任务由一个以"dts"为后缀的配置文件定义，配置文件目录默认为"./conf.d"，
如果需要改变可通过java -Ddts.conf_dir=XXX命令行传参方式修改。
配置文件说明如下：
```
{
    "source":{
        "sql":"数据导出SQL，可包括占位符“?”",
        "args":[
            "SQL参数1",
            "参数也可使用内置参数，内置参数见“注1”"
        ]
    },
    "sink":{
        "hbase":{
            "table_name":"HBase表名",
            "familys":[
                {
                    "family_name":"列簇名",
                    "block_size":"列簇BlockSize大小，默认为64KB",
                    "columns":[
                        "字段名，对应source的数据结构"
                    ]
                }
            ],
            "default_family":{
                "family_name":"默认列簇名（包含其它列簇未定义的字段，default_family可以空，则未定义列簇字段不导入HBase）",
                "block_size":"列簇BlockSize大小，默认为64KB"
            },
            "row_key":{
                "pattern":"RowKey生成规则，可包括占位符“?”（例：id_?_date_?，占位符参数取自args）",
                "args":[
                    "RowKey生成参数，参数分为字段参数和内置参数，内置参数见“注1”"
                ]
            }
        }
    }
}
```

## 注
### 注1 内置参数表

内置参数名 | 说明
---|---
{timestamp} | 当前时间戳（精确到秒）
{long_timestamp} | 当前时间戳（精确到毫秒）
{date} | 当天日期（2018-11-01）
{date:yyyy/MM/dd hh:mm:ss} | 当天自定义格式日期（2018/11/01 13:45:11）
{last_date} | 昨天日期（2018-11-01）
{last_date:yyyy/MM/dd hh:mm:ss} | 昨天自定义格式日期（2018/11/01）
{random_char:N} | 随机字符串（字符范围0-9, A-Z, a-z）


## 下个版本计划
1. 增加dts文件内Channel配置
2. 增加dts文件内JDBC数据源配置
3. 增加ElasticSearch导入功能
4. 增加导入完成事件，支持导入完成后执行SQL和Shell脚本
5. 增加dts文件内HBase配置