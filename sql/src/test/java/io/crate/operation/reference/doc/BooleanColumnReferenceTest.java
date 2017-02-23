/*
 * Licensed to Crate.io Inc. (Crate) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file to
 * you under the Apache License, Version 2.0 (the "License");  you may not
 * use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, to use any modules in this file marked as "Enterprise Features",
 * Crate must have given you permission to enable and use such Enterprise
 * Features and you must have a valid Enterprise or Subscription Agreement
 * with Crate.  If you enable or use the Enterprise Features, you represent
 * and warrant that you have a valid Enterprise or Subscription Agreement
 * with Crate.  Your use of the Enterprise Features if governed by the terms
 * and conditions of your Enterprise or Subscription Agreement with Crate.
 */

package io.crate.operation.reference.doc;

import io.crate.operation.reference.doc.lucene.BooleanColumnReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class BooleanColumnReferenceTest extends DocLevelExpressionsTest {

    private String column = "b";

    @Override
    protected void insertValues(IndexWriter writer) throws Exception {
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(new StringField("_id", Integer.toString(i), Field.Store.NO));
            doc.add(new NumericDocValuesField(column, i % 2 == 0 ? 1 : 0));
            writer.addDocument(doc);
        }
    }

    @Test
    public void testBooleanExpression() throws Exception {
        BooleanColumnReference booleanColumn = new BooleanColumnReference(column);
        booleanColumn.startCollect(ctx);
        booleanColumn.setNextReader(readerContext);
        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 20);
        int i = 0;
        for (ScoreDoc doc : topDocs.scoreDocs) {
            booleanColumn.setNextDocId(doc.doc);
            assertThat(booleanColumn.value(), is(i % 2 == 0));
            i++;
        }
    }
}
