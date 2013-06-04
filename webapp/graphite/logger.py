"""Copyright 2008 Orbitz WorldWide

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import os, logging
from logging.handlers import TimedRotatingFileHandler as Rotater
from django.conf import settings

logging.addLevelName(30,"rendering")
logging.addLevelName(30,"cache")
logging.addLevelName(30,"metric_access")

class GraphiteLogger:
  def __init__(self):
    #Setup log files
    self.defaultLogFile = os.path.join(settings.LOG_DIR,"graphite.log")
    #self.infoLogFile = os.path.join(settings.LOG_DIR,"info.log")
    self.exceptionLogFile = os.path.join(settings.LOG_DIR,"exception.log")
    self.cacheLogFile = os.path.join(settings.LOG_DIR,"cache.log")
    self.renderingLogFile = os.path.join(settings.LOG_DIR,"rendering.log")
    self.metricAccessLogFile = os.path.join(settings.LOG_DIR,"metricaccess.log")
    #Setup loggers
    self.defaultLogger = logging.getLogger("default")
    self.defaultLogger.setLevel(logging.DEBUG)
    #self.infoLogger = logging.getLogger("info")
    #self.infoLogger.setLevel(logging.INFO)
    self.exceptionLogger = logging.getLogger("exception")
    self.cacheLogger = logging.getLogger("cache")
    self.renderingLogger = logging.getLogger("rendering")
    self.metricAccessLogger = logging.getLogger("metric_access")
    #Setup formatter & handlers
    self.formatter = logging.Formatter("%(asctime)s :: %(message)s","%a %b %d %H:%M:%S %Y")
    self.defaultHandler = Rotater(self.defaultLogFile,when="midnight",backupCount=1)
    self.defaultHandler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s","%a %b %d %H:%M:%S %Y"))
    self.defaultLogger.addHandler(self.defaultHandler)
    #self.infoHandler = Rotater(self.infoLogFile,when="midnight",backupCount=1)
    #self.infoHandler.setFormatter(self.formatter)
    #self.infoLogger.addHandler(self.infoHandler)
    
    self.exceptionHandler = Rotater(self.exceptionLogFile,when="midnight",backupCount=1)
    self.exceptionHandler.setFormatter(self.formatter)
    self.exceptionLogger.addHandler(self.exceptionHandler)
    if settings.LOG_CACHE_PERFORMANCE:
      self.cacheHandler = Rotater(self.cacheLogFile,when="midnight",backupCount=1)
      self.cacheHandler.setFormatter(self.formatter)
      self.cacheLogger.addHandler(self.cacheHandler)
    if settings.LOG_RENDERING_PERFORMANCE:
      self.renderingHandler = Rotater(self.renderingLogFile,when="midnight",backupCount=1)
      self.renderingHandler.setFormatter(self.formatter)
      self.renderingLogger.addHandler(self.renderingHandler)
    if settings.LOG_METRIC_ACCESS:
      self.metricAccessHandler = Rotater(self.metricAccessLogFile,when="midnight",backupCount=10)
      self.metricAccessHandler.setFormatter(self.formatter)
      self.metricAccessLogger.addHandler(self.metricAccessHandler)

  def debug(self,msg,*args,**kwargs):
    return self.defaultLogger.debug(msg,*args,**kwargs)
  
  def info(self,msg,*args,**kwargs):
    return self.defaultLogger.info(msg,*args,**kwargs)
  
  def warn(self,msg,*args,**kwargs):
    return self.defaultLogger.warn(msg,*args,**kwargs)
  
  def error(self,msg,*args,**kwargs):
    return self.defaultLogger.error(msg,*args,**kwargs)

  def exception(self,msg="Exception Caught",**kwargs):
    return self.exceptionLogger.exception(msg,**kwargs)

  def cache(self,msg,*args,**kwargs):
    return self.cacheLogger.log(30,msg,*args,**kwargs)

  def rendering(self,msg,*args,**kwargs):
    return self.renderingLogger.log(30,msg,*args,**kwargs)

  def metric_access(self,msg,*args,**kwargs):
    return self.metricAccessLogger.log(30,msg,*args,**kwargs)


log = GraphiteLogger() # import-shared logger instance
