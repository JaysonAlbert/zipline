![image](http://www.zipline-live.io/images/zipline_live.png)

zipline-live
============

[![pypi
badge](https://badge.fury.io/py/cn-zipline-live.svg)](https://pypi.python.org/pypi/cn-zipline-live)
[![version
status](https://img.shields.io/pypi/pyversions/cn-zipline-live.svg)](https://pypi.python.org/pypi/cn-zipline-live)
![travis
status](https://travis-ci.org/JaysonAlbert/zipline.svg?branch=master%0A%20:target:%20https://travis-ci.org/JaysonAlbert/zipline)
[![appveyor
status](https://ci.appveyor.com/api/projects/status/fc6rgyckxj445uf5?svg=true)](https://ci.appveyor.com/project/JaysonAlbert/zipline/branch/master)
[![Apache
License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

使用文档已经迁移到gitbook上，请戳
[这里](https://www.gitbook.com/book/jaysonalbert/zipline/details)

`cn-zipline-live` 是一个支持实盘的股票回测框架，是基于 `zipline-live`
的二次开发，以支持国内市场。
[zipline-live](https://github.com/zipline-live/zipline/) 是基于
`zipline` 二次开发的回测框架，使用盈透证券（ib）的实盘接口。

[zipline](http://zipline.io/) 是美国
[Quantopian](https://quantopian.com/) 公司开源的量化交易回测引擎，它使用
`Python` 语言开发， 部分代码使用 `cython` 融合了部分c语言代码。
`Quantopian` 在它的网站上的回测系统就是基于 `zipline` 的，
经过生产环境的长期使用，已经比完善，并且在持续的改进中。

`zipline` 的基本使用方法在
<https://www.quantopian.com/tutorials/getting-started/>
,对于zipline的深度解析，可以看大神 [rainx](https://github.com/rainx)
写的 [文档](https://www.gitbook.com/book/rainx/-zipline/details)
，本项目中的大部分依赖项目也都是rainx开发的项目

cn-zipline-live主要分为三个模块，回测，实盘以及研究，其中研究模块正处于初步开发中，功能尙待完善。

数据源
------

`cn-zipline-live`
的历史k线以及除息除权数据来自通达信，数据接口来自项目github 项目
[tdx](https://github.com/JaysonAlbert/tdx)

安装
----

> pip install cn-zipline-live

实盘
----

1.  配置:

> 准备好配置文件zipline/gens/example\_config.json, 以及trader.dll

2.  运行：

> 情况1：
> :   win32
>     python：将配置文件以及dll放入策略所在目录，修改配置文件名（默认应为config.json，见live\_strategy）,然后运行live\_strategy。
>
> 情况2：

>   其它环境（win64 python或者linux
> python）：将配置文件、dll以及tdx\_client.exe(文件过大无法上传到git，见QQ群文件)放到同一目录，并运行tdx\_client.exe，然后在live\_strategy中修改相应的uri，运行live\_strategy。

使用
----

cn-zipline-live与zipline大同小异，具体使用方法请参考zipline
[官方文档](https://www.quantopian.com/tutorials/getting-started) 。

一、ingest数据：
----------------

> zipline ingest -b tdx -a assets.csv --minute False --start 20170901
> --overwrite True

`-a assets.csv` 指定需要 `ingest` 的代码列表，缺省ingest
4000+只所有股票，耗时长达3、4小时，通过 `-a tests/ETF.csv` 只ingest
ETF基金数据，一方面可以节省时间达到快速测试的目的。
另一方面可以通过这种方法ingest非股票数据，例如etf基金。

`--minute False` 是否ingest分钟数据

`--start 20170901` 数据开始日期，默认为1991年

`--overwrite True`
由于分钟数据获取速度较慢，默认start至今超过3年的话，只拿3年数据，日线数据依然以start为准，overwrite为True时，强制拿从start开始
至今的分钟数据

二、编写策略以及运行策略： -----------

请参考目录: `zipline/examples`

问题
----

如有任何问题，欢迎大家提交
[issue](https://github.com/JaysonAlbert/zipline/issues/new/)
，反馈bug，以及提出改进建议。

其它
----

对量化感兴趣的朋友，以及想更方便的交流朋友，请加QQ群434588628
