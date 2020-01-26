test=$1
imin=$2
imax=$3
nprof=$4
for i in $(seq "$imin" "$imax"); do
	fname="run_query.py -v $i -t $test -n 3"
	python $fname
done
