for i in `seq 1 9`;
do /home/vagrant/rdfs/build/rice-datanode/datanode -p 5002$i -x 5001$i &
done
