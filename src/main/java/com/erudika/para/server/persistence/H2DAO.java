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
 * Fake DAO for in-memory persistence.
 * Used for testing and development without a database.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class H2DAO implements DAO {

	private static final Logger logger = LoggerFactory.getLogger(H2DAO.class);

	static {
		if (H2DAO.class.getSimpleName().equals(Para.getConfig().daoPlugin())) {
			// set up automatic table creation and deletion
			App.addAppCreatedListener((App app) -> {
				if (app != null) {
					H2Utils.createTable(app.getAppIdentifier());
				}
			});
			App.addAppDeletedListener((App app) -> {
				if (app != null) {
					H2Utils.deleteTable(app.getAppIdentifier());
				}
			});
		}
	}

	/**
	 * Default constructor.
	 */
	public H2DAO() {
	}

	@Override
	public <P extends ParaObject> String create(String appid, P object) {
		if (object == null) {
			return null;
		}
		H2Utils.createRows(appid, Collections.singletonList(object));
		logger.debug("DAO.create() {}", object.getId());
		return object.getId();
	}

	@Override
	@SuppressWarnings("unchecked")
	public <P extends ParaObject> P read(String appid, String key) {
		if (key == null || StringUtils.isBlank(appid)) {
			return null;
		}
		Map<String, P> results = H2Utils.readRows(appid, Collections.singletonList(key));
		P object = results.get(key);
		logger.debug("DAO.read() {} -> {}", key, object);
		return object;
	}

	@Override
	public <P extends ParaObject> void update(String appid, P object) {
		if (object != null && !StringUtils.isBlank(appid)) {
			H2Utils.updateRows(appid, Collections.singletonList(object));
			logger.debug("DAO.update() {}", object.getId());
		}
	}

	@Override
	public <P extends ParaObject> void delete(String appid, P object) {
		if (object != null && !StringUtils.isBlank(appid)) {
			H2Utils.deleteRows(appid, Collections.singletonList(object));
			logger.debug("DAO.delete() {}", object.getId());
		}
	}

	@Override
	public <P extends ParaObject> void createAll(String appid, List<P> objects) {
		if (StringUtils.isBlank(appid) || objects == null) {
			return;
		}
		H2Utils.createRows(appid, objects);
		logger.debug("DAO.createAll() {}", objects.size());
	}

	@Override
	@SuppressWarnings("unchecked")
	public <P extends ParaObject> Map<String, P> readAll(String appid, List<String> keys, boolean getAllColumns) {
		if (keys == null || StringUtils.isBlank(appid)) {
			return Collections.emptyMap();
		}
		Map<String, P> results = H2Utils.readRows(appid, keys);
		logger.debug("DAO.readAll() {}", results.size());
		return results;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <P extends ParaObject> List<P> readPage(String appid, Pager pager) {
		return H2Utils.scanRows(appid, pager);
	}

	@Override
	public <P extends ParaObject> void updateAll(String appid, List<P> objects) {
		if (!StringUtils.isBlank(appid) && objects != null) {
			H2Utils.updateRows(appid, objects);
			logger.debug("DAO.updateAll() {}", objects.size());
		}
	}

	@Override
	public <P extends ParaObject> void deleteAll(String appid, List<P> objects) {
		if (!StringUtils.isBlank(appid) && objects != null) {
			H2Utils.deleteRows(appid, objects);
			logger.debug("DAO.deleteAll() {}", objects.size());
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
