SET /p env=Conda environment name for datajoint tools:  

CALL conda activate base
CALL conda env remove -n %env% -y
CALL conda deactivate
CALL conda env create -f environment.yml -n %env%
CALL conda activate %env%