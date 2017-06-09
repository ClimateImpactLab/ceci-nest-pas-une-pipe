#!/bin/bash
# Job name:
#SBATCH --job-name=resid
#
# Partition:
#SBATCH --partition=savio2
#
# Account:
#SBATCH --account=co_laika
#
# QoS:
#SBATCH --qos=savio_lowprio
#
#SBATCH --nodes=1
#
#SBATCH --ntasks-per-node=24
#
#SBATCH  --cpus-per-task=1
#
# Wall clock limit:
#SBATCH --time=72:00:00
#
#SBATCH --array=0-288

## Run command
python web_seasonal_temp.py --job_id ${SLURM_ARRAY_TASK_ID}