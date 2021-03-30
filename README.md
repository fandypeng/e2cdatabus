# e2cdatabus
This is a data bus for excel2config, it receive data from excel2config when your database is quarantined.

# 简介
e2cdatabus是excel2config项目的一个辅助工具项目，如果你希望使用Excel来管理配置，但是部署excel2config项目的服务器又无法访问你的数据库，
那么你可以通过在能读写数据库的项目中集成e2cdatabus来接收Excel的配置数据，由e2cdatabus将配置数据写入数据库。

