/*
 * Copyright 2013-2021 Erudika. https://erudika.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For issues and patches go to: https://github.com/erudika
 */
package com.erudika.para.server.persistence;

import com.erudika.para.server.persistence.SqlDAO;
import com.erudika.para.server.persistence.SqlUtils;
import org.apache.commons.lang3.StringUtils;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Perform an integration test of the generic SQL DAO using a specific mode.
 * @author Jeremy Wiesner [jswiesner@gmail.com]
 */
@Ignore
public abstract class SqlDAOIT extends DAOTest {

	private static final String ROOT_APP_NAME = "para-test";

	public SqlDAOIT(String mode) {
		super(new SqlDAO());
		org.h2.Driver.load();
		System.setProperty("para.sql.driver", "org.h2.Driver");
		System.setProperty("para.sql.url", "h2:mem:test" + (StringUtils.isBlank(mode) ? "" : ";MODE=" + mode));
		System.setProperty("para.sql.user", "user");
		System.setProperty("para.sql.password", "password");
		initialize();
	}

	private void initialize() {
		SqlUtils.createTable(ROOT_APP_NAME);
		SqlUtils.createTable(appid1);
		SqlUtils.createTable(appid2);
		SqlUtils.createTable(appid3);
	}

	@AfterClass
	public static void destroy() {
		SqlUtils.deleteTable(ROOT_APP_NAME);
		SqlUtils.deleteTable(appid1);
		SqlUtils.deleteTable(appid2);
		SqlUtils.deleteTable(appid3);
		SqlUtils.shutdownClient();
	}

	@Test
	public void testCreateDeleteExistsTable() throws InterruptedException {
		String testappid1 = "test-index";
		String badAppid = "test index 123";

		SqlUtils.createTable("");
		assertFalse(SqlUtils.existsTable(""));

		SqlUtils.createTable(testappid1);
		assertTrue(SqlUtils.existsTable(testappid1));

		SqlUtils.deleteTable(testappid1);
		assertFalse(SqlUtils.existsTable(testappid1));

		assertFalse(SqlUtils.createTable(badAppid));
		assertFalse(SqlUtils.existsTable(badAppid));
		assertFalse(SqlUtils.deleteTable(badAppid));
	}

}
