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

import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.persistence.DAO;
import com.erudika.para.core.utils.Pager;
import com.erudika.para.core.utils.Para;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic SQL DAO plugin usable with a wide range of SQL implementations.
 * @author Jeremy Wiesner [jswiesner@gmail.com]
 */
public class SqlDAO implements DAO {

	private static final Logger logger = LoggerFactory.getLogger(SqlDAO.class);

	static {
		if (SqlDAO.class.getSimpleName().equals(Para.getConfig().daoPlugin())) {
			// set up automatic table creation and deletion
			App.addAppCreatedListener((App app) -> {
				if (app != null) {
					SqlUtils.createTable(app.getAppIdentifier());
				}
			});
			App.addAppDeletedListener((App app) -> {
				if (app != null) {
					SqlUtils.deleteTable(app.getAppIdentifier());
				}
			});
		}
	}

	/**
	 * Default constructor.
	 */
	public SqlDAO() {
	}

	@Override
	public <P extends ParaObject> String create(String appid, P object) {
		if (object == null) {
			return null;
		}
		SqlUtils.createRows(appid, Collections.singletonList(object));
		logger.debug("SqlDAO.create() {}", object.getId());
		return object.getId();
	}

	@Override
	@SuppressWarnings("unchecked")
	public <P extends ParaObject> P read(String appid, String key) {
		if (key == null || StringUtils.isBlank(appid)) {
			return null;
		}
		Map<String, P> results = SqlUtils.readRows(appid, Collections.singletonList(key));
		P object = results.get(key);
		logger.debug("SqlDAO.read() {} -> {}", key, object);
		return object;
	}

	@Override
	public <P extends ParaObject> void update(String appid, P object) {
		if (object != null && !StringUtils.isBlank(appid)) {
			SqlUtils.updateRows(appid, Collections.singletonList(object));
			logger.debug("SqlDAO.update() {}", object.getId());
		}
	}

	@Override
	public <P extends ParaObject> void delete(String appid, P object) {
		if (object != null && !StringUtils.isBlank(appid)) {
			SqlUtils.deleteRows(appid, Collections.singletonList(object));
			logger.debug("SqlDAO.delete() {}", object.getId());
		}
	}

	@Override
	public <P extends ParaObject> void createAll(String appid, List<P> objects) {
		if (StringUtils.isBlank(appid) || objects == null) {
			return;
		}
		SqlUtils.createRows(appid, objects);
		logger.debug("SqlDAO.createAll() {}", objects.size());
	}

	@Override
	@SuppressWarnings("unchecked")
	public <P extends ParaObject> Map<String, P> readAll(String appid, List<String> keys, boolean getAllColumns) {
		if (keys == null || StringUtils.isBlank(appid)) {
			return Collections.emptyMap();
		}
		Map<String, P> results = SqlUtils.readRows(appid, keys);
		logger.debug("SqlDAO.readAll() {}", results.size());
		return results;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <P extends ParaObject> List<P> readPage(String appid, Pager pager) {
		List<P> results = SqlUtils.readPage(appid, pager);
		logger.debug("SqlDAO.readPage() {}", results.size());
		return results;
	}

	@Override
	public <P extends ParaObject> void updateAll(String appid, List<P> objects) {
		if (!StringUtils.isBlank(appid) && objects != null) {
			SqlUtils.updateRows(appid, objects);
			logger.debug("SqlDAO.updateAll() {}", objects.size());
		}
	}

	@Override
	public <P extends ParaObject> void deleteAll(String appid, List<P> objects) {
		if (!StringUtils.isBlank(appid) && objects != null) {
			SqlUtils.deleteRows(appid, objects);
			logger.debug("SqlDAO.deleteAll() {}", objects.size());
		}
	}

	////////////////////////////////////////////////////////////////////

	@Override
	public <P extends ParaObject> String create(P object) {
		return create(Para.getConfig().getRootAppIdentifier(), object);
	}

	@Override
	public <P extends ParaObject> P read(String key) {
		return read(Para.getConfig().getRootAppIdentifier(), key);
	}

	@Override
	public <P extends ParaObject> void update(P object) {
		update(Para.getConfig().getRootAppIdentifier(), object);
	}

	@Override
	public <P extends ParaObject> void delete(P object) {
		delete(Para.getConfig().getRootAppIdentifier(), object);
	}

	@Override
	public <P extends ParaObject> void createAll(List<P> objects) {
		createAll(Para.getConfig().getRootAppIdentifier(), objects);
	}

	@Override
	public <P extends ParaObject> Map<String, P> readAll(List<String> keys, boolean getAllColumns) {
		return readAll(Para.getConfig().getRootAppIdentifier(), keys, getAllColumns);
	}

	@Override
	public <P extends ParaObject> List<P> readPage(Pager pager) {
		return readPage(Para.getConfig().getRootAppIdentifier(), pager);
	}

	@Override
	public <P extends ParaObject> void updateAll(List<P> objects) {
		updateAll(Para.getConfig().getRootAppIdentifier(), objects);
	}

	@Override
	public <P extends ParaObject> void deleteAll(List<P> objects) {
		deleteAll(Para.getConfig().getRootAppIdentifier(), objects);
	}

}
