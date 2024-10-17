
# A股涨跌幅预测系统
[点击访问博客](https://chinastockpredict.blogspot.com/)
用户协议:本系统仅作为学习交流使用,您不应当将本系统的预测结果作为投资建议,本人将尽力确保项目内数据数据及时更新,但在外部情况的干扰下(如:运营商服务暂停,github访问受阻等)

本系统当前专用于当日**涨跌幅最低点**的预测，简称预低
本项目默认使用python3.10
深度学习框架使用pytorch
预测方法:(dfhist.csv+list.csv->stock.csv)
1.**每个交易日的9:30至9:40之间我会更新本项目中的部分文件如下：**
	dfhist.csv
	dflist.csv
2.**使用者拉取更新后,在本文件夹内运行stock.py,产生stock.csv文件**
3.**查看新生成的stock.csv文件,其中包含预测值**
注意:确保拉取的dfhist.csv和list.csv文件是最新的
检验方法:
1.在收盘后运行verify.py,得到verify.csv文件(涵盖全部预测结果对比)
2.运行check.py文件得到部分预测值以及误差(缩量+预低<0)

本系统特性如下:
在缩量+预低<0的情况下有很好的预测效果,长期观测平均误差的绝对值<0.8‣ു�