cd schedule
for f in *.py
do
    echo "Updating ~/airflow/dags/${f}..."
    cat $f | sed 's/\*\*\*@gmail/adeelkhan11@gmail/g' > ~/airflow/dags/$f
done
echo "Done"
