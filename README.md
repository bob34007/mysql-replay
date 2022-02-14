# mysql-replay
read network packets from the production environment, parse them and persist
the production and simulation result sets in a file after playback from the
simulation environment

# 仿真工具
读取生产环境的网络包，解析网络包并在仿真环境执行后将生产环境结果集和仿真环境结果集持久化道文件中


# demo 
./mysql-replay dir replay -D"/Users/pcapfile/" -d"root:test34007@tcp(192.168.1.189:4002)/test"  --storeDir "/Users/ouput/" -o "/Users/store/" --log-output="/Users/mysql-replay-11.log" --filesize 100 -t 800