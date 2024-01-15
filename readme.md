## Setup env

Use conda to setup virtual python environment on each machine.

```shell
conda create --name env-name python=3.7
```

Enter the project root directory and install dependencies

```shell
cd /path/to/proj
pip install -r requirements.txt
python -m pip install "dask[complete]"    # Install everything
```

Add the path of SA to the path of python on each machine:

```shell
export PYTHONPATH=$PYTHONPATH:/path/to/proj/lib:/path/to/proj/pycomposer/
```

Prepare data for benchmark `birth analysis`

```shell
./get_data.sh
```

Change the line 8 of file `/path/to/proj/birth_analysis/run.sh` to your own dataset path.

## Setup Cluster

Run the following code on the scheduler

```shell
dask-scheduler
```

Run the following code on each worker node

```
dask-worker tcp://<SCHEDULER-ADDRESS> --no-nanny --worker-port <PORT> --name=<NAME>
```

## Run Benchmark

Change the parameters of the Cluster() function in each python file to the address of your scheduler. For example in``/path/to/proj/birth_analysis/birth_analysis_dask.py` line 120 :

```python
client = Client('tcp://<SCHEDULER-ADDRESS>')
```

Then, run each `run.sh` under each benchmark folder.

For example:

```shell
cd birth_analysis
./run.sh
```

