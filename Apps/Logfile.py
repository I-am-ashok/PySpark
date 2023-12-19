def log(name):
    import logging
    from logging import FileHandler
    logformat="%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(level=logging.INFO,format=logformat)
    logfile=f"Logs/{name}.log"
    logfileFileHandler=FileHandler(logfile,mode='w')
    fileformat=logging.Formatter(logformat)
    logfileFileHandler.setFormatter(fileformat)
    logfileFileHandler.setLevel(logging.INFO)
    logging.getLogger().addHandler(logfileFileHandler)
    return logging