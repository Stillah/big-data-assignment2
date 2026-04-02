# big-data-assignment2

# How to run
## Step 1: Install prerequisites
- Docker
- Docker compose
## Step 2: Run the command
```bash
docker compose up 
```


This will create 3 containers, a master node and a worker node for Hadoop, and Cassandra server. The master node will run the script `app/app.sh` as an entrypoint.

Issues may come out related to memory allocation during index creation. I personally had to add this into yarn-site.xml:

<property>                                                                                                                                                                            
  <name>yarn.nodemanager.resource.memory-mb</name>                                                                                                                                    
  <value>2200</value>                                                                                                                                                                 
  <description>Amount of physical memory, in MB, that can be allocated for containers.</description>                                                                                  
</property>                                                                                                                                                                           
                                                                                                                                                                                      
<property>                                                                                                                                                                            
  <name>yarn.scheduler.minimum-allocation-mb</name>                                                                                                                                   
  <value>500</value>                                                                                                                                                                  
</property> 
