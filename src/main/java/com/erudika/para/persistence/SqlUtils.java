/*
 * Copyright 2013-2017 Erudika. https://erudika.com
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

package com.erudika.para.persistence;

import com.erudika.para.annotations.Locked;
import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.utils.ParaObjectUtils;
import com.erudika.para.utils.Config;
import com.erudika.para.utils.Pager;
import com.erudika.para.utils.Utils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.HashMap;

/**
 * Helper utilities for performing generic SQL operations using the JDBC library.
 * @author Jeremy Wiesner [jswiesner@gmail.com]
 */
public final class SqlUtils {

	private static final Logger logger = LoggerFactory.getLogger(SqlUtils.class);
	private static final String JSON_FIELD_NAME = "json";
	private static final String TABLE_SCHEMA = Utils.formatMessage(
		"{0} NVARCHAR(64) PRIMARY KEY NOT NULL," +
		"{1} NVARCHAR(64) NOT NULL," +
		"{2} NVARCHAR(64) DEFAULT NULL," +
		"{3} TIMESTAMP NOT NULL," +
		"{4} TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"{5} LONGTEXT NOT NULL",
		Config._ID,
		Config._TYPE,
		Config._CREATORID,
		Config._TIMESTAMP,
		Config._UPDATED,
		JSON_FIELD_NAME
	);

	private static HikariDataSource hikariDataSource;
	private static boolean useMySqlSyntax = false;

	private SqlUtils() { }

	/**
	 * Returns a connection to the SQL database.
	 * @return a connection instance
	 */
	static Connection getConnection() throws SQLException {
		if (hikariDataSource != null) {
			return hikariDataSource.getConnection();
		}

		String sqlUrl = Config.getConfigParam("sql.url", null);
		String sqlDriver = Config.getConfigParam("sql.driver", null);
		String sqlUser = Config.getConfigParam("sql.user", "user");
		String sqlPassword = Config.getConfigParam("sql.password", "secret");

		if (StringUtils.isBlank(sqlUrl)) {
			logger.error("Missing required configuration parameter \"para.sql.url\" for the SqlDAO");
		}
		if (Config.getConfigParam("sql.driver", null) == null) {
			logger.error("Missing required configuration parameter \"para.sql.driver\" for the SqlDAO");
		}

		// verify the SQL driver can be loaded from the classpath
		try {
			Class.forName(sqlDriver);
			useMySqlSyntax = sqlDriver.contains("mysql");
		} catch (ClassNotFoundException e) {
			logger.error("Error loading SQL driver \"" + sqlDriver + "\", class not found.");
			return null;
		}

		// verify a connection can be made to the SQL server
		try {
			Connection conn = DriverManager.getConnection("jdbc:" + sqlUrl, sqlUser, sqlPassword);
			conn.close();
		} catch (SQLException e) {
			logger.error("Failed to connect to SQL database: " + e.getMessage());
			return null;
		}

		// connection and driver are valid, so establish a connection pool
		HikariConfig hikariConfig = new HikariConfig();
		hikariConfig.setJdbcUrl("jdbc:" + sqlUrl);
		hikariConfig.setUsername(sqlUser);
		hikariConfig.setPassword(sqlPassword);
		hikariConfig.setDriverClassName(sqlDriver);
		/*
		hikariConfig.addDataSourceProperty("cachePrepStmts", Config.getConfigBoolean("sql.cachePrepStmts"), true);
		hikariConfig.addDataSourceProperty("prepStmtCacheSize", Config.getConfigInt("sql.prepStmtCacheSize"), 500);
		hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", Config.getConfigInt("sql.prepStmtCacheSqlLimit"), 2048);
		hikariConfig.addDataSourceProperty("useServerPrepStmts", Config.getConfigBoolean("sql.useServerPrepStmts", true));
		*/
		hikariDataSource = new HikariDataSource(hikariConfig);

		if (!existsTable(Config.getRootAppIdentifier())) {
			createTable(Config.getRootAppIdentifier());
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				shutdownClient();
			}
		});

		return hikariDataSource.getConnection();
	}

	/**
	 * Stops the connection pool and releases resources.
	 * <b>There's no need to call this explicitly!</b>
	 */
	protected static void shutdownClient() {
		if (hikariDataSource != null) {
			hikariDataSource.close();
			hikariDataSource = null;
		}
	}

	/**
	 * Checks if a specific table exists in the SQL database.
	 * @param appid name of the {@link com.erudika.para.core.App}
	 * @return true if the table exists
	 */
	public static boolean existsTable(String appid) {
		if (StringUtils.isBlank(appid)) {
			return false;
		}
		try (Connection connection = getConnection()) {
			PreparedStatement ps = connection.prepareStatement(
					"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?");
			ps.setString(1, getTableNameForAppid(appid).toUpperCase());
			ResultSet res = ps.executeQuery();
			if (res.next()) {
				String name = res.getString(1);
				return name != null;
			}
		} catch (Exception e) {
			logger.error("Failed to check if table exists for appid '{}'{}", appid, logSqlError(e));
		}
		return false;
	}

	/**
	 * Creates a new table in the SQL database.
	 * @param appid name of the {@link com.erudika.para.core.App}
	 * @return true if created
	 */
	public static boolean createTable(String appid) {
		if (StringUtils.isBlank(appid) || StringUtils.containsWhitespace(appid) || existsTable(appid)) {
			return false;
		}
		try (Connection connection = getConnection()) {
			String tableName = getTableNameForAppid(appid);
			connection.createStatement()
					.execute(Utils.formatMessage("CREATE TABLE IF NOT EXISTS {0} ({1})", tableName, TABLE_SCHEMA));
			logger.info("Created SQL database table named '{}'.", tableName);
			return true;
		} catch (Exception e) {
			logger.error("Failed to create a new table for appid '{}' in the SQL database{}", appid, logSqlError(e));
		}
		return false;
	}

	/**
	 * Deletes a table in the SQL database if it exists.
	 * @param appid id of the {@link com.erudika.para.core.App}
	 * @return true if deleted
	 */
	public static boolean deleteTable(String appid) {
		if (StringUtils.isBlank(appid) || !existsTable(appid)) {
			return false;
		}
		try (Connection connection = getConnection()) {
			String tableName = getTableNameForAppid(appid);
			connection.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
			logger.info("Deleted table named '{}' from the SQL database.", tableName);
		} catch (Exception e) {
			logger.error("Failed to delete the table for appid '{}' in the SQL database{}", appid, logSqlError(e));
		}
		return true;
	}

	/**
	 * Returns the table name for a given app id. Table names are usually in the form 'prefix-appid'.
	 * @param appIdentifier app id
	 * @return the table name
	 */
	public static String getTableNameForAppid(String appIdentifier) {
		if (StringUtils.isBlank(appIdentifier)) {
			return "";
		} else {
			return ((App.isRoot(appIdentifier) ||
					appIdentifier.startsWith(Config.PARA.concat("-"))) ?
					appIdentifier : Config.PARA + "-" + appIdentifier).replaceAll("-", "_"); // SQLs don't like "-"
		}
	}

	/**
	 * Converts rows from the SQL database to a map of {@link ParaObject}s.
	 * @param <P> type of object
	 * @param appid app id
	 * @param ids row ids
	 * @return a list populated Para objects.
	 */
	protected static <P extends ParaObject> Map<String, P> readRows(String appid, List<String> ids) {
		if (StringUtils.isBlank(appid) || ids == null || ids.isEmpty()) {
			return Collections.emptyMap();
		}
		try (Connection connection = getConnection()) {
			Map<String, P> results = new LinkedHashMap<>();
			String tableName = getTableNameForAppid(appid);
			PreparedStatement ps = connection.prepareStatement(Utils.formatMessage(
					"SELECT {0} FROM {1} WHERE {2} IN ({3})",
					JSON_FIELD_NAME,
					tableName,
					Config._ID,
					StringUtils.repeat("?", ",", ids.size())));
			for (int i = 0; i < ids.size(); i++) {
				ps.setString(i + 1, ids.get(i));
				results.put(ids.get(i), null);
			}
			ResultSet res = ps.executeQuery();
			while (res.next()) {
				P obj = ParaObjectUtils.fromJSON(res.getString(1));
				if (obj != null) {
					results.put(obj.getId(), obj);
				}
			}
			return results;
		} catch (Exception e) {
			logger.error("Failed to read rows for appid '{}' in the SQL database{}", appid, logSqlError(e));
		}
		return Collections.emptyMap();
	}

	/**
	 * Converts a list of {@link ParaObject}s to SQL database rows and inserts them.
	 * @param <P> type of object
	 * @param appid app id
	 * @param objects list of ParaObjects
	 */
	protected static <P extends ParaObject> void createRows(String appid, List<P> objects) {
		if (StringUtils.isBlank(appid) || objects == null || objects.isEmpty()) {
			return;
		}
		try (Connection connection = getConnection()) {
			String tableName = getTableNameForAppid(appid);
			PreparedStatement ps;
			if (useMySqlSyntax) {
				ps = connection.prepareStatement(Utils.formatMessage("INSERT INTO {0} VALUES (?,?,?,?,?,?) " +
						"ON DUPLICATE KEY UPDATE {1}=?,{2}=?",
						tableName,
						Config._UPDATED,
						JSON_FIELD_NAME));
			} else {
				ps = connection.prepareStatement("MERGE INTO " + tableName +	" VALUES (?,?,?,?,?,?)");
			}

			for (P object : objects) {
				if (StringUtils.isBlank(object.getId())) {
					object.setId(Utils.getNewId());
				}
				if (object.getTimestamp() == null) {
					object.setTimestamp(Utils.timestamp());
				}
				object.setAppid(appid);
				final Timestamp createTimestamp = object.getTimestamp() == null ?
						new Timestamp(System.currentTimeMillis()) : new Timestamp(object.getTimestamp());
				final Timestamp updateTimestamp = object.getUpdated() == null ?
						createTimestamp : new Timestamp(object.getUpdated());
				final String objectJson = ParaObjectUtils.getJsonWriterNoIdent().
						writeValueAsString(ParaObjectUtils.getAnnotatedFields(object, false));

				ps.setString(1, object.getId());
				ps.setString(2, object.getType());
				ps.setString(3, object.getCreatorid());
				ps.setTimestamp(4, createTimestamp);
				ps.setTimestamp(5, updateTimestamp);
				ps.setString(6, objectJson);
				if (useMySqlSyntax) {
					ps.setTimestamp(7, updateTimestamp);
					ps.setString(8, objectJson);
				}
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (Exception e) {
			logger.error("Failed to create rows for appid '{}' in the SQL database{}", appid, logSqlError(e), e);
		}
	}

	/**
	 * Converts a list of {@link ParaObject}s to SQL database rows and updates them.
	 * @param <P> type of object
	 * @param appid app id
	 * @param objects a list of ParaObjects
	 */
	protected static <P extends ParaObject> void updateRows(String appid, List<P> objects) {
		if (StringUtils.isBlank(appid) || objects == null || objects.isEmpty()) {
			return;
		}
		try (Connection connection = getConnection()) {
			String tableName = getTableNameForAppid(appid);
			Map<String, P> objectsMap = new HashMap<>(objects.size());
			for (P object : objects) {
				if (object != null && !StringUtils.isBlank(object.getId())) {
					object.setUpdated(Utils.timestamp());
					objectsMap.put(object.getId(), object);
				}
			}

			Map<String, P> existingObjects = readRows(appid, new ArrayList<>(objectsMap.keySet()));

			PreparedStatement ps = connection.prepareStatement(Utils.formatMessage(
					"UPDATE {0} SET {1}=?,{2}=? WHERE {3} = ?",
					tableName,
					Config._UPDATED,
					JSON_FIELD_NAME,
					Config._ID));

			for (P existingObject : existingObjects.values()) {
				if (existingObject != null) {
					P object = objectsMap.get(existingObject.getId());
					Map<String, Object> data = ParaObjectUtils.getAnnotatedFields(object, false);
					P updated = ParaObjectUtils.setAnnotatedFields(existingObject, data, Locked.class);

					final Timestamp updateTimestamp = object.getUpdated() == null ?
							new Timestamp(System.currentTimeMillis()) : new Timestamp(object.getUpdated());
					ps.setTimestamp(1, updateTimestamp);
					ps.setString(2, ParaObjectUtils.getJsonWriterNoIdent().
							writeValueAsString(ParaObjectUtils.getAnnotatedFields(updated, false)));
					ps.setString(3, updated.getId());
					ps.addBatch();
				}
			}
			ps.executeBatch();
		} catch (Exception e) {
			logger.error("Failed to update rows for appid '{}' in the SQL database{}", appid, logSqlError(e));
		}
	}

	/**
	 * Deletes a list of {@link ParaObject}s from a SQL database table.
	 * @param <P> type of object
	 * @param appid app id
	 * @param objects a list of ParaObjects
	 */
	protected static <P extends ParaObject> void deleteRows(String appid, List<P> objects) {
		if (StringUtils.isBlank(appid) || objects == null || objects.isEmpty()) {
			return;
		}
		try (Connection connection = getConnection()) {
			String tableName = getTableNameForAppid(appid);
			PreparedStatement ps = connection.prepareStatement(Utils.formatMessage(
					"DELETE FROM {0} WHERE {1} IN ({2})",
					tableName,
					Config._ID,
					StringUtils.repeat("?", ",", objects.size())));
			for (int i = 0; i < objects.size(); i++) {
				ps.setString(i + 1, objects.get(i).getId());
			}
			ps.execute();
		} catch (Exception e) {
			logger.error("Failed to delete rows for appid '{}' in the SQL database{}", appid, logSqlError(e));
		}
	}

	/**
	 * Reads one page from the DB.
	 * @param <P> type of object
	 * @param appid app id
	 * @param pager a {@link Pager}
	 * @return a list of ParaObjects
	 */
	protected static <P extends ParaObject> List<P> readPage(String appid, Pager pager) {
		if (StringUtils.isBlank(appid)) {
			return Collections.emptyList();
		}
		if (pager == null) {
			pager = new Pager();
		}
		try (Connection connection = getConnection()) {
			List<P> results = new ArrayList<>(pager.getLimit());
			int start = pager.getPage() <= 1 ? 0 : (int) (pager.getPage() - 1) * pager.getLimit();
			String tableName = getTableNameForAppid(appid);
			PreparedStatement p = connection.prepareStatement(Utils.formatMessage(
					"SELECT ROWNUM(), {0} FROM (SELECT {1} FROM {2}) WHERE ROWNUM() > ? LIMIT ?",
					JSON_FIELD_NAME,
					JSON_FIELD_NAME,
					tableName));
			p.setInt(1, start);
			p.setInt(2, pager.getLimit());
			ResultSet res = p.executeQuery();
			int i = 0;
			while (res.next()) {
				P obj = ParaObjectUtils.fromJSON(res.getString(2));
				if (obj != null) {
					results.add(obj);
					pager.setLastKey(obj.getId());
					i++;
				}
			}
			pager.setCount(pager.getCount() + i);
			if (pager.getPage() < 2) {
				pager.setPage(2);
			} else {
				pager.setPage(pager.getPage() + 1);
			}
			return results;
		} catch (Exception e) {
			logger.error("Failed to read page for appid '{}' from the SQL database{}", appid, logSqlError(e));
		}
		return Collections.emptyList();
	}

	private static String logSqlError(Exception e) {
		if (e == null || !SQLException.class.isAssignableFrom(e.getClass())) {
			return "";
		}
		SQLException sqlException = (SQLException) e;
		return " (Error Code: " + sqlException.getErrorCode() + ", SQLState: " + sqlException.getSQLState() + ")";
	}
}
