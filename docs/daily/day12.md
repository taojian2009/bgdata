### day 12

#### 切片不足的时候的策略

当切片的时候，当后面的加起来 <=12.8MB的话，就不用。就把剩余的所有切片当做一个 整体。

#### 分区

分区主要是为了减轻每个分区的数据压力。 避免数据倾斜。 用groupby.  
多个reduce, 数据分散开，更大。

#### combiner

在提交给reducer前，提前对数据局部聚合。提交执行效率。 减少IO和网络开销，提升性能。 combiner慎用！ 在hive中，其实只需要调一个参数。

#### 分组

分组:将相同K2的v2放在集合中。

[hello, hello] => [1, 1]

为啥要自定义分组？？

k2,

JavaBean.

```bash 
张三 138 80 
张三 139 75

[(张三 138)， (张三 139)] => [80, 75]
```

一般根据key找value.

案例: 求组订单中，金额最高的订单。

原始数据=>行偏移量，k1, v1 => 组内排序，对成交金额。

通过Bean里面的compareTO 返回值分组。

[(a, 1), (a, 1)] -> [90, 80]

`JobMain`
`OrderBean`
`OrderMapper`
`OrderReducer`
`OrderGroupComparator`

`compareTO`:

+ 排序的时候需要调用
+ 如果没有指定分组，调用此方法

#### 综合案例

用MapReduce做数据清洗。

后续基本都用`HiveSQL`, `SparkSQL`, 和`FlinkSQL`了









