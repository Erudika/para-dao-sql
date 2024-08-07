/*
 * Copyright 2013-2022 Erudika. https://erudika.com
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

import com.erudika.para.core.utils.Para;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class H2DAOIT extends DAOTest {

	private static final String ROOT_APP_NAME = "para-test";

	public H2DAOIT() {
		super(new H2DAO());
	}

	@BeforeAll
	public static void setUpClass() throws InterruptedException {
		System.setProperty("para.app_name", ROOT_APP_NAME);
		H2Utils.createTable(Para.getConfig().getRootAppIdentifier());
		H2Utils.createTable(appid1);
		H2Utils.createTable(appid2);
		H2Utils.createTable(appid3);
	}

	@AfterAll
	public static void tearDownClass() {
//		H2Utils.deleteTable(Para.getConfig().getRootAppIdentifier());
		H2Utils.deleteTable(appid1);
		H2Utils.deleteTable(appid2);
		H2Utils.deleteTable(appid3);
	}

}
