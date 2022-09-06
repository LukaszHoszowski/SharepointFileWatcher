## SFW - SharepointFileWatcher

## What is it?

**SFW** is a simple, filewatcher application wrote in Python 3. App allow you to monitor 
local files or files located on the **MS Sharepoints** in given time interval. Each time when
some change in modified date property of the file will be detected app can trigger, attached
to this file, function. In my use case scenario I was monitoring this source files, and once 
any of the file has been changed app was triggering Pandas ETL operations. App supports 
multiprocessing and logs (app can capture logs from separate processes as well).

## Main Features
Here is the list of major features:

  - **Local files monitoring**: program can monitor single or multiple files in given directory 
based on latest modify date of the file. 
  - **Sharepoint files monitoring**: program can monitor single or multiple sharepoint files
  - **File name pattern mechanism**: program has ability to recognize files on pattern basis using **' \* '**
  - **Independent processes**: each time once change in files is detected program will dispatch attached function is separate process
  - **Easy configuration**: program is equipped with simple configuration file which will allow you to quickly configure entire pipeline
  - **Logging mechanism**: program has implemented logging mechanism which will collect logs from all processes and save them into single file

## Dependencies
- [Office365-REST-Python-Client - Adds support for all sharepoint operations](https://pypi.org/project/Office365-REST-Python-Client/)
- [python-dotenv - Simplifies using system environment variables](https://pypi.org/project/python-dotenv/)

## Installation
To install you need to clone the repository and fill in **'config.ini'** and **'.env'**. Once you complete config files just drop functions to **'executables'**.

- **config.ini**: keeps almost the entire setup of the program, paths, file names, sharepoint details. In example config file 
'config-default.ini' you will find sample data with schema of config file. Just enter your details.
- **.env**: is reserved for most sensitive data, the secrets. I've decided to keep them in separate place to easily move this envs to secrets if you decide to run the script in Docker and to make it a bit safer

Each file (**source**) has some function (**action**) attached. All of actions are stored in **'executables'** package folder. Name of your **source** needs to correspond with import address of your **executable**.
For example: If my **action** is located in **example.py** with name **test** (name of the function). The name of your **source** needs to be set as **'example.test'**. To avoid adding more and more variables to config I've decide to use **source** names as **action** names. Such move also helps in debugging.

**Working modes**: app can work in two modes: **'sharepoint'** or **'local'**.
- **'sharepoint'** will prioritize sharepoint connections at first, if program will not be able to retrieve list of files or establish connection will try to look for that file in local resources
- **'local'** will focus on local resources, sharepoint connection will not be considered at all

Once you configure the program just add **requirements.txt** and run **main.py**.

Go to main folder of application and run below commands:

```sh
pip install -r requirements.txt
```

```sh
python3 main.py
```

## How the program works

Once you run `main.py`: 
1) app will create instance of **Logger Process**. This object will be responsible to intercept all logs from other processes.
2) app will try to parse **'config.ini'** and **'.env'** files. If app works in **'sharepoint'** mode during config parsing app will try to establish connection with the sharepoints and create `ctx` connector objects upfront.
3) `While True` loop starts here. Program will do the first cycle and try to retrieve modify dates of **sources** from previous run, program will try to read `./logs/dir_modified_dates.pickle`, if no success program will consider this as a true first start.
4) Program will compare current modify dates of **'sources'** with previous once. If there are any discrepancies all changed **'sources'** will be added to the list of items which needs to be processed: `sources2refresh`.
5) If `sources2refresh` is empty program will start next loop, else program will create separate process for each item from the list and execute them concurrently. Once all the processes finish, program will start next loop. 

## License
[Apache 2.0](LICENSE)
