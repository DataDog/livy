/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy.rsc.driver;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;
import com.cloudera.livy.JobHandle;
import com.cloudera.livy.rsc.BypassJobStatus;
import com.cloudera.livy.rsc.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class JobWrapper<T> implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(JobWrapper.class);

  public final String jobId;

  private final RSCDriver driver;
  private final Job<T> job;
  private final String className;
  private final String[] arguments;
  private final AtomicInteger completed;

  private volatile T result;
  protected volatile Throwable error;
  protected volatile JobHandle.State state;

  private Future<?> future;

  JobWrapper(RSCDriver driver, String jobId, Job<T> job) {
    this.driver = driver;
    this.jobId = jobId;
    this.job = job;
    this.completed = new AtomicInteger();

    this.className = null;
    this.arguments = null;

    state = JobHandle.State.QUEUED;
  }

  JobWrapper(RSCDriver driver, String jobId, String className, String[] arguments) {
    this.driver = driver;
    this.jobId = jobId;
    this.className = className;
    this.arguments = arguments;
    this.completed = new AtomicInteger();

    this.job = null;

    state = JobHandle.State.QUEUED;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Void call() throws Exception {
    try {
      driver.jobContext().sc().setJobGroup(jobId, jobId);
      LOG.info("Job group " + jobId + " started");

      jobStarted();
      state = JobHandle.State.STARTED;

      if (job != null) {
        result = job.call(driver.jobContext());
      } else {
        result = (T) Class.forName(className).getMethod("call", JobContext.class, String[].class)
             .invoke(null, driver.jobContext(), arguments);
      }

      finished(result, null);
    } catch (Throwable t) {
      // Catch throwables in a best-effort to report job status back to the client. It's
      // re-thrown so that the executor can destroy the affected thread (or the JVM can
      // die or whatever would happen if the throwable bubbled up).
      LOG.info("Failed to run job " + jobId, t);
      finished(null, t);
      throw new ExecutionException(t);
    } finally {
      driver.activeJobs.remove(jobId);
    }
    return null;
  }

  void submit(ExecutorService executor) {
    this.future = executor.submit(this);
  }

  public Job<T> getJob() {
    return job;
  }

  void jobDone() {
    synchronized (completed) {
      completed.incrementAndGet();
      completed.notifyAll();
    }
  }

  boolean cancel() {
    driver.jobContext().sc().cancelJobGroup(jobId);
    state = JobHandle.State.CANCELLED;
    return true;
  }

  protected void finished(T result, Throwable error) {
    if (error == null) {
      LOG.info("Job group " + jobId + " finished");
      driver.jobFinished(jobId, result, null);
      state = JobHandle.State.SUCCEEDED;
    } else {
      LOG.info("Job group " + jobId + " failed");
      driver.jobFinished(jobId, null, error);
      state = JobHandle.State.FAILED;
    }
  }

  protected void jobStarted() {
    driver.jobStarted(jobId);
  }

  public static byte[] serialize(Object obj) throws IOException {
    try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
      try(ObjectOutputStream o = new ObjectOutputStream(b)){
        o.writeObject(obj);
      }
      return b.toByteArray();
    }
  }

  synchronized BypassJobStatus getStatus() {
    String stackTrace = error != null ? Utils.stackTraceAsString(error) : null;
    try {
      return new BypassJobStatus(state, serialize(result), stackTrace);
    } catch (IOException e) {
      return new BypassJobStatus(state, null, stackTrace);
    }
  }
}
