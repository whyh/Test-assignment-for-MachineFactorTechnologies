FROM jupyter/minimal-notebook:latest

COPY requirements.txt .
RUN pip install --quiet --no-cache-dir -r requirements.txt

# Starts Jupyter notebook without auth (fix for my PyCharm integration)
CMD start-notebook.sh --NotebookApp.token=''