# Information regarding resource tuning can be found under http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/

# Check number of physical processors
cat /proc/cpuinfo | grep "physical id" | sort -u | wc -l

# Total number of cores
nproc

# Threads per core
lscpu | grep -i thread

# Check CPU usage per core
mpstat -P ALL 1