/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler;

import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.CreateSnapshot;
import org.apache.solr.client.solrj.request.CoreAdminRequest.DeleteSnapshot;
import org.apache.solr.client.solrj.request.CoreAdminRequest.ListSnapshots;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrSnapshotMetaDataManager.SnapshotMetaData;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SolrTestCaseJ4.SuppressSSL // Currently unknown why SSL does not work with this test
public class TestSolrCoreSnapshots extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static long docsSeed; // see indexDocs()

  @BeforeClass
  public static void setupClass() throws Exception {
    useFactory("solr.StandardDirectoryFactory");
    configureCluster(1)// nodes
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    docsSeed = random().nextLong();
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    System.clearProperty("test.build.data");
    System.clearProperty("test.cache.data");
  }

  @Test
  public void testBackupRestore() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String collectionName = "SolrCoreSnapshots";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1);
    create.process(solrClient);

    String location = createTempDir().toFile().getAbsolutePath();
    int nDocs = BackupRestoreUtils.indexDocs(cluster.getSolrClient(), collectionName, docsSeed);

    DocCollection collectionState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    assertEquals(1, collectionState.getActiveSlices().size());
    Slice shard = collectionState.getActiveSlices().iterator().next();
    assertEquals(1, shard.getReplicas().size());
    Replica replica = shard.getReplicas().iterator().next();

    String replicaBaseUrl = replica.getStr(BASE_URL_PROP);
    String coreName = replica.getStr(ZkStateReader.CORE_NAME_PROP);
    String backupName = TestUtil.randomSimpleString(random(), 1, 5);
    String commitName = TestUtil.randomSimpleString(random(), 1, 5);

    try (
        SolrClient adminClient = getHttpSolrClient(cluster.getJettySolrRunners().get(0).getBaseUrl().toString());
        SolrClient masterClient = getHttpSolrClient(replica.getCoreUrl())) {

      SnapshotMetaData metaData = createSnapshot(adminClient, coreName, commitName);

      // Delete all documents
      masterClient.deleteByQuery("*:*");
      masterClient.commit();
      BackupRestoreUtils.verifyDocs(0, cluster.getSolrClient(), collectionName);

      // Backup the earlier created snapshot.
      {
        Map<String,String> params = new HashMap<>();
        params.put("name", backupName);
        params.put("commitName", commitName);
        params.put("location", location);
        runCoreAdminCommand(replicaBaseUrl, coreName, CoreAdminAction.BACKUPCORE.toString(), params);
      }

      // Restore the backup
      {
        Map<String,String> params = new HashMap<>();
        params.put("name", "snapshot." + backupName);
        params.put("location", location);
        runCoreAdminCommand(replicaBaseUrl, coreName, CoreAdminAction.RESTORECORE.toString(), params);
        BackupRestoreUtils.verifyDocs(nDocs, cluster.getSolrClient(), collectionName);
      }

      // Delete the snapshot
      deleteSnapshot(adminClient, coreName, commitName);

      // Verify that corresponding index files have been deleted.
      assertTrue(listCommits(metaData.getIndexDirPath()).isEmpty());
    }
  }

  @Test
  public void testIndexOptimization() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String collectionName = "SolrCoreSnapshots_IndexOptimization";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1);
    create.process(solrClient);

    int nDocs = BackupRestoreUtils.indexDocs(cluster.getSolrClient(), collectionName, docsSeed);

    DocCollection collectionState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    assertEquals(1, collectionState.getActiveSlices().size());
    Slice shard = collectionState.getActiveSlices().iterator().next();
    assertEquals(1, shard.getReplicas().size());
    Replica replica = shard.getReplicas().iterator().next();

    String coreName = replica.getStr(ZkStateReader.CORE_NAME_PROP);
    String commitName = TestUtil.randomSimpleString(random(), 1, 5);

    try (
        SolrClient adminClient = getHttpSolrClient(cluster.getJettySolrRunners().get(0).getBaseUrl().toString());
        SolrClient masterClient = getHttpSolrClient(replica.getCoreUrl())) {

      SnapshotMetaData metaData = createSnapshot(adminClient, coreName, commitName);

      int numTests = nDocs > 0 ? TestUtil.nextInt(random(), 1, 5) : 1;
      for (int attempts=0; attempts<numTests; attempts++) {
        //Modify existing index before we call optimize.
        if (nDocs > 0) {
          //Delete a few docs
          int numDeletes = TestUtil.nextInt(random(), 1, nDocs);
          for(int i=0; i<numDeletes; i++) {
            masterClient.deleteByQuery("id:" + i);
          }
          //Add a few more
          int moreAdds = TestUtil.nextInt(random(), 1, 100);
          for (int i=0; i<moreAdds; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", i + nDocs);
            doc.addField("name", "name = " + (i + nDocs));
            masterClient.add(doc);
          }
          masterClient.commit();
        }
      }

      // Before invoking optimize command, verify that the index directory contains multiple commits (including the one we snapshotted earlier).
      {
        Collection<IndexCommit> commits = listCommits(metaData.getIndexDirPath());
        // Verify that multiple index commits are stored in this directory.
        assertTrue(commits.size() > 0);
        // Verify that the snapshot commit is present in this directory.
        assertTrue(commits.stream().filter(x -> x.getGeneration() == metaData.getGenerationNumber()).findFirst().isPresent());
      }

      // Optimize the index.
      masterClient.optimize(true, true, 1);

      // After invoking optimize command, verify that the index directory contains multiple commits (including the one we snapshotted earlier). 
      {
        SimpleFSDirectory dir = new SimpleFSDirectory(Paths.get(metaData.getIndexDirPath()));
        List<IndexCommit> commits = DirectoryReader.listCommits(dir);
        // Verify that multiple index commits are stored in this directory.
        assertTrue(commits.size() > 1);
        // Verify that the snapshot commit is present in this directory.
        assertTrue(commits.stream().filter(x -> x.getGeneration() == metaData.getGenerationNumber()).findFirst().isPresent());
      }

      // Delete the snapshot
      deleteSnapshot(adminClient, coreName, metaData.getName());

      // Add few documents. Without this the optimize command below does not take effect.
      {
        int moreAdds = TestUtil.nextInt(random(), 1, 100);
        for (int i=0; i<moreAdds; i++) {
          SolrInputDocument doc = new SolrInputDocument();
          doc.addField("id", i + nDocs);
          doc.addField("name", "name = " + (i + nDocs));
          masterClient.add(doc);
        }
        masterClient.commit();
      }

      // Optimize the index.
      masterClient.optimize(true, true, 1);

      // Verify that the index directory contains only 1 index commit (which is not the same as the snapshotted commit).
      Collection<IndexCommit> commits = listCommits(metaData.getIndexDirPath());
      assertTrue(commits.size() == 1);
      assertFalse(commits.stream().filter(x -> x.getGeneration() == metaData.getGenerationNumber()).findFirst().isPresent());
    }
  }

  private SnapshotMetaData createSnapshot (SolrClient adminClient, String coreName, String commitName) throws Exception {
    CreateSnapshot req = new CreateSnapshot(commitName);
    req.setCoreName(coreName);
    adminClient.request(req);

    Collection<SnapshotMetaData> snapshots = listSnapshots(adminClient, coreName);
    Optional<SnapshotMetaData> metaData = snapshots.stream().filter(x -> commitName.equals(x.getName())).findFirst();
    assertTrue(metaData.isPresent());

    return metaData.get();
  }

  private void deleteSnapshot(SolrClient adminClient, String coreName, String commitName) throws Exception {
    DeleteSnapshot req = new DeleteSnapshot(commitName);
    req.setCoreName(coreName);
    adminClient.request(req);

    Collection<SnapshotMetaData> snapshots = listSnapshots(adminClient, coreName);
    assertFalse(snapshots.stream().filter(x -> commitName.equals(x.getName())).findFirst().isPresent());
  }

  private Collection<SnapshotMetaData> listSnapshots(SolrClient adminClient, String coreName) throws Exception {
    ListSnapshots req = new ListSnapshots();
    req.setCoreName(coreName);
    NamedList resp = adminClient.request(req);
    assertTrue( resp.get("snapshots") instanceof NamedList );
    NamedList apiResult = (NamedList) resp.get("snapshots");

    List<SnapshotMetaData> result = new ArrayList<>(apiResult.size());
    for(int i = 0 ; i < apiResult.size(); i++) {
      String commitName = apiResult.getName(i);
      String indexDirPath = (String)((NamedList)apiResult.get(commitName)).get("indexDirPath");
      long genNumber = Long.valueOf((String)((NamedList)apiResult.get(commitName)).get("generation"));
      result.add(new SnapshotMetaData(commitName, indexDirPath, genNumber));
    }
    return result;
  }

  private Collection<IndexCommit> listCommits(String directory) throws Exception {
    SimpleFSDirectory dir = new SimpleFSDirectory(Paths.get(directory));
    try {
      return DirectoryReader.listCommits(dir);
    } catch (IndexNotFoundException ex) {
      // This can happen when the delete snapshot functionality cleans up the index files (when the directory
      // storing these files is not the *current* index directory).
      return Collections.emptyList();
    }
  }

  static void runCoreAdminCommand(String baseUrl, String coreName, String action, Map<String,String> params)
      throws IOException {
    StringBuilder builder = new StringBuilder();
    builder.append(baseUrl);
    builder.append("/admin/cores?action=");
    builder.append(action);
    builder.append("&core=");
    builder.append(coreName);
    for (Map.Entry<String,String> p : params.entrySet()) {
      builder.append("&");
      builder.append(p.getKey());
      builder.append("=");
      builder.append(p.getValue());
    }
    String masterUrl = builder.substring(0, builder.length());
    InputStream stream = null;
    try {
      URL url = new URL(masterUrl);
      stream = url.openStream();
      stream.close();
    } finally {
      IOUtils.closeQuietly(stream);
    }
  }
}