Introduction
============

What is pylytics?
*****************

pylytics is a tool for processing data into a star schema. The industry term for this is an ETL tool (extract, transform, load).

* Extract - pull the data from a range of different sources (SQL, flat files, APIs).
* Transform - clean the data, and prepare it for populating fact and dimension tables.
* Load - create the fact and dimension tables in a datawarehouse, and populate them with the cleaned data.

What are star schemas?
**********************

A star schema is a way of storing data which it is intended to be analysed.

There are many data visualisation tools on the market which work with star schemas. However, creating the star schema can be tricky in the first place, and this is what pylytics helps to make easier.

A star schema consists of two types of tables - `facts` and `dimensions`. Each fact table represents something you're interesting in measuring / recording. An example is sales in a retail store.

The things you want to record are known as `metric` columns in pylytics (though other tools may refer to them as `measures`). An example metric is sales amount. Metrics are numeric values, which will be summed, averaged, and counted by the visualisation tool.

Fact tables also consist of dimension columns, which contain foreign keys to dimension tables. Dimension tables describe the data being recorded. Examples are store, manager, and date. Dimensions are used to filter and group the metrics in useful ways.

A fact table will likely only contain one or two metric columns, but can contain dozens of dimension columns. This allows for powerful analysis.

Using the online store example, you can easily do queries such as 'show me the total sales of shampoo, for the New York store, during December, when the manager was Tom Plank'.

The more dimension tables you create, the easier it is to make future fact tables. A fact table could be created which records retail stock, and this could reuse a lot of the dimensions created for the retail sales fact (e.g. store, date).
