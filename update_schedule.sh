cd schedule
for f in *.py
do
    echo "Updating ~/airflow/dags/${f}..."
    cat $f | sed 's/\*\*\*@gmail/adeelkhan11@gmail/g' | sed 's-~/venv/bin/python3-python3-g' > ~/airflow/dags/$f
done
echo "Done"
