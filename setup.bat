SET env=datajoint

CALL conda activate base
CALL conda env remove -n %env% -y
CALL conda env create -f environment.yml -n %env%
CALL conda activate %env%