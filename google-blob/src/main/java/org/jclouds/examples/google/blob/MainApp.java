/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.jclouds.examples.google.blob;


import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;

import org.jclouds.ContextBuilder;
import org.jclouds.apis.ApiMetadata;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.compute.ComputeService;
import org.jclouds.domain.Credentials;
import org.jclouds.googlecloud.GoogleCredentialsFromJson;

import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;

/**
 * Demonstrates the use of {@link ComputeService}.
 * <p/>
 * Usage is:
 * {@code java MainApp provider identity credential groupName (add|exec|run|destroy)}
 * if {@code exec} is used, the following parameter is a command, which should
 * be passed in quotes
 * if {@code run} is used, the following parameter is a file to execute.
 */
public class MainApp {

   public static int PARAMETERS = 1;
   public static String INVALID_SYNTAX = "Invalid number of parameters. Syntax is: json_key_path";

   public static void main(String[] args) throws IOException, InterruptedException {
      if (args.length < PARAMETERS)
         throw new IllegalArgumentException(INVALID_SYNTAX);

      System.out.println("Sucessfully Running!");
      String jsonKeyFile = args[0];

      String containerName = "test-container-1";
      String blobName = "test";
      PrintWriter writer = new PrintWriter("blog-ping.log", "UTF-8");

      // Read in JSON key.
      String fileContents = null;
      try {
         fileContents = Files.toString(new File(jsonKeyFile), Charset.defaultCharset());
      } catch (IOException ex){
         System.out.println("Error Reading the Json key file. Please check the provided path is correct.");
         System.exit(1);
      }
      System.out.println(fileContents);

      Supplier<Credentials> credentialSupplier = new GoogleCredentialsFromJson(fileContents);

      BlobStoreContext context = ContextBuilder.newBuilder("google-cloud-storage")
            .credentialsSupplier(credentialSupplier)
            .buildView(BlobStoreContext.class);

      System.out.println("Constructed context!");

      try {
         // Create Container
         BlobStore blobStore = context.getBlobStore();
         blobStore.createContainerInLocation(null, containerName);
         ByteSource payload = ByteSource.wrap("testdata".getBytes(Charsets.UTF_8));

         // List Container Metadata
         for (StorageMetadata resourceMd : blobStore.list()) {
            if (containerName.equals(resourceMd.getName())) {
               System.out.println(resourceMd);
            }
         }

         // Add Blob
         Blob blob = blobStore.blobBuilder(blobName)
            .payload(payload)
            .contentLength(payload.size())
            .build();
         blobStore.putBlob(containerName, blob);

         ApiMetadata apiMetadata = context.unwrap().getProviderMetadata().getApiMetadata();
         System.out.println("apiMetadata");
         System.out.println(apiMetadata);

         int iteration = 0;
         while(true){
            Blob gotBlob = blobStore.getBlob(containerName, blobName);
            System.out.printf("time:%d iteration:%d blob:%s \n", System.currentTimeMillis(), iteration++, gotBlob);
            writer.printf("time:%d iteration:%d blob:%s \n", System.currentTimeMillis(), iteration, gotBlob);
            writer.flush();
            Thread.sleep(1000*60*10); // 10 Minutes.
         }



      } finally {
         writer.close();
         context.close();
         System.exit(0);
      }
   }
}
