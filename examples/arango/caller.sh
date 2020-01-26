test=$1
imin=$2
imax=$3
for i in $(seq "$imin" "$imax"); do
	fname="run_query.py -v $i -t $test"
	python "fname"
done