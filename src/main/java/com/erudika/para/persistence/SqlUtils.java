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

package com.erudika.para.persistence;

import com.erudika.para.DestroyListener;
import com.erudika.para.Para;
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
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Helper utilities for performing generic SQL operations using the JDBC library.
 * @author Jeremy Wiesner [jswiesner@gmail.com]
 */
public final class SqlUtils {

	private static final Logger logger = LoggerFactory.getLogger(SqlUtils.class);
	private static final String JSON_FIELD_NAME = "json";
	private static final String JSON_UPDATES_FIELD_NAME = "json_updates";
	private static HikariDataSource hikariDataSource;
	private static boolean useMySqlSyntax = false;
	private static boolean useMSSqlSyntax = false;
	private static boolean usePGSqlSyntax = false;
	private static boolean useOrSqlSyntax = false;
	private static boolean useLiSqlSyntax = false;

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
			useMySqlSyntax = StringUtils.containsAny(sqlDriver, "mysql", "mariadb");
			useMSSqlSyntax = sqlDriver.contains("sqlserver");
			usePGSqlSyntax = sqlDriver.contains("postgresql");
			useOrSqlSyntax = sqlDriver.contains("oracle");
			useLiSqlSyntax = sqlDriver.contains("sqlite");
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
		hikariDataSource = new HikariDataSource(hikariConfig);

		if (!existsTable(Config.getRootAppIdentifier())) {
			createTable(Config.getRootAppIdentifier());
		}

		Para.addDestroyListener(new DestroyListener() {
			public void onDestroy() {
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

	private static String getTableSchema() {
		return Utils.formatMessage(
			"{0} {5} PRIMARY KEY NOT NULL," +
			"{1} {5} NOT NULL," +
			"{2} {5} DEFAULT NULL," +
			"{3} {6} NOT NULL," +
			"{4} {6} DEFAULT NULL",
			Config._ID,
			Config._TYPE,
			Config._CREATORID,
			JSON_FIELD_NAME,
			JSON_UPDATES_FIELD_NAME,
			useMSSqlSyntax ? "NVARCHAR(255)" :
					(useMySqlSyntax ? "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" :
							(useOrSqlSyntax ? "NVARCHAR2(255)" : "VARCHAR(255)")),
			useMSSqlSyntax ? "NVARCHAR(MAX)" : (useOrSqlSyntax ? "NCLOB" : "TEXT")
		);
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
			if (connection == null) {
				return false;
			}
			try (PreparedStatement ps = getInfoTablePreparedStatement(connection)) {
				ps.setString(1, getTableNameForAppid(appid));
				ps.setString(2, getTableNameForAppid(appid).toUpperCase());
				try (ResultSet res = ps.executeQuery()) {
					return res.next() && res.getString(1) != null;
				}
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
			if (connection == null) {
				return false;
			}
			String tableName = getTableNameForAppid(appid);
			try	(Statement ps = connection.createStatement()) {
				ps.execute(Utils.formatMessage(
							"CREATE TABLE {0} ({1}){2}",
							tableName,
							getTableSchema(),
							useMySqlSyntax ? " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" : ""));
			}
			logger.info("Created SQL database table named '{}'.", tableName);
			return true;
		} catch (Exception e) {
			if (useMySqlSyntax) {
				createTableForLegacyMySQL(appid);
			} else {
				logger.error("Failed to create a new table for appid '{}' in the SQL database{}", appid, logSqlError(e));
			}
		}
		return false;
	}

	/**
	 * Legacy MySQL <= 5.6 has a key limit of 767 bytes.
	 * https://stackoverflow.com/questions/1814532/1071-specified-key-was-too-long-max-key-length-is-767-bytes
	 * https://github.com/Erudika/para/issues/41
	 * @param appid app id
	 * @return true if created
	 */
	private static boolean createTableForLegacyMySQL(String appid) {
		try (Connection connection = getConnection()) {
			if (connection == null) {
				return false;
			}
			String tableSchema = getTableSchema().replace("VARCHAR(255)", "VARCHAR(190)");
			String tableName = getTableNameForAppid(appid);
			try (Statement ps = connection.createStatement()) {
				ps.execute(Utils.formatMessage("CREATE TABLE {0} ({1}){2}", tableName, tableSchema,
						" CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"));
			}
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
			if (connection == null) {
				return false;
			}
			String tableName = getTableNameForAppid(appid);
			try	(Statement ps = connection.createStatement()) {
				ps.execute(Utils.formatMessage("DROP TABLE {0}", tableName));
			}
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
			if (connection == null) {
				return Collections.emptyMap();
			}
			Map<String, P> results = new LinkedHashMap<>();
			String tableName = getTableNameForAppid(appid);
			try	(PreparedStatement ps = connection.prepareStatement(Utils.formatMessage(
					"SELECT {0},{1} FROM {2} WHERE {3} IN ({4})", JSON_FIELD_NAME, JSON_UPDATES_FIELD_NAME,
					tableName, Config._ID, StringUtils.repeat("?", ",", ids.size())))) {

				for (int i = 0; i < ids.size(); i++) {
					ps.setString(i + 1, ids.get(i));
					results.put(ids.get(i), null);
				}
				try (ResultSet res = ps.executeQuery()) {
					while (res.next()) {
						P obj = ParaObjectUtils.fromJSON(res.getString(1));
						if (obj != null) {
							if (res.getString(2) != null) {
								results.put(obj.getId(), ParaObjectUtils.setAnnotatedFields(obj,
										ParaObjectUtils.getJsonReader(Map.class).readValue(res.getString(2)), null));
							} else {
								results.put(obj.getId(), obj);
							}
						}
					}
					return results;
				}
			}
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
			if (connection == null) {
				return;
			}
			String tableName = getTableNameForAppid(appid);
			try (PreparedStatement ps = getUpsertRowPreparedStatement(connection, tableName)) {
				for (P object : objects) {
					if (StringUtils.isBlank(object.getId())) {
						object.setId(Utils.getNewId());
					}
					if (object.getTimestamp() == null) {
						object.setTimestamp(Utils.timestamp());
					}
					object.setAppid(appid);
					final String objectJson = ParaObjectUtils.getJsonWriterNoIdent().
							writeValueAsString(ParaObjectUtils.getAnnotatedFields(object, false));

					ps.setString(1, object.getId());
					ps.setString(2, object.getType());
					if (object.getCreatorid() != null) {
						ps.setString(3, object.getCreatorid());
					} else {
						ps.setNull(3, Types.NULL);
					}
					ps.setString(4, objectJson);

					if (useMySqlSyntax || usePGSqlSyntax || useLiSqlSyntax) {
						ps.setString(5, object.getType());
						ps.setString(6, object.getCreatorid());
						ps.setString(7, objectJson);
					} else if (useMSSqlSyntax) {
						ps.setString(5, object.getId());
						ps.setString(6, object.getId());
						ps.setString(7, object.getType());
						ps.setString(8, object.getCreatorid());
						ps.setString(9, objectJson);
					}
					ps.addBatch();
				}
				ps.executeBatch();
			}
		} catch (Exception e) {
			logger.error("Failed to create rows for appid '{}' in the SQL database{}", appid, logSqlError(e), e);
			throwIfNecessary(e);
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
			if (connection == null) {
				return;
			}
			String tableName = getTableNameForAppid(appid);
			String sql = Utils.formatMessage("UPDATE {0} SET {1}=? WHERE {2} = ?",
					tableName, JSON_UPDATES_FIELD_NAME, Config._ID);

			try (PreparedStatement ps = connection.prepareStatement(sql)) {
				for (P object : objects) {
					if (object != null && !StringUtils.isBlank(object.getId())) {
						object.setUpdated(Utils.timestamp());
						Map<String, Object> data = ParaObjectUtils.getAnnotatedFields(object, Locked.class, false);
						ps.setString(1, ParaObjectUtils.getJsonWriterNoIdent().writeValueAsString(data));
						ps.setString(2, object.getId());
						ps.addBatch();
					}
				}
				ps.executeBatch();
			}
		} catch (Exception e) {
			logger.error("Failed to update rows for appid '{}' in the SQL database{}", appid, logSqlError(e));
			throwIfNecessary(e);
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
			if (connection == null) {
				return;
			}
			String tableName = getTableNameForAppid(appid);
			try (PreparedStatement ps = connection.prepareStatement(Utils.formatMessage(
					"DELETE FROM {0} WHERE {1} IN ({2})", tableName, Config._ID,
					StringUtils.repeat("?", ",", objects.size())))) {

				for (int i = 0; i < objects.size(); i++) {
					ps.setString(i + 1, objects.get(i).getId());
				}
				ps.execute();
			}
		} catch (Exception e) {
			logger.error("Failed to delete rows for appid '{}' in the SQL database{}", appid, logSqlError(e));
			throwIfNecessary(e);
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
			if (connection == null) {
				return Collections.emptyList();
			}
			List<P> results = new ArrayList<>(pager.getLimit());
			int start = pager.getPage() <= 1 ? 0 : (int) (pager.getPage() - 1) * pager.getLimit();
			String tableName = getTableNameForAppid(appid);
			try (PreparedStatement p = getReadPagePreparedStatement(connection, tableName)) {
				p.setInt(useOrSqlSyntax ? 2 : 1, pager.getLimit());
				p.setInt(useOrSqlSyntax ? 1 : 2, start);
				try (ResultSet res = p.executeQuery()) {
					int i = 0;
					while (res.next()) {
						P obj = ParaObjectUtils.fromJSON(res.getString(1));
						if (obj != null) {
							if (res.getString(2) != null) {
								results.add(ParaObjectUtils.setAnnotatedFields(obj,
										ParaObjectUtils.getJsonReader(Map.class).readValue(res.getString(2)), null));
							} else {
								results.add(obj);
							}
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
				}
			}
		} catch (Exception e) {
			logger.error("Failed to read page for appid '{}' from the SQL database{}", appid, logSqlError(e));
		}
		return Collections.emptyList();
	}

	private static PreparedStatement getInfoTablePreparedStatement(Connection connection) throws SQLException {
		 if (useLiSqlSyntax) {
			return connection.prepareStatement("SELECT tbl_name FROM sqlite_master WHERE tbl_name = ? OR tbl_name = ?");
		}
		return connection.prepareStatement(Utils.formatMessage("SELECT TABLE_NAME FROM {0} WHERE "
				+ "TABLE_NAME = ? OR TABLE_NAME = ?", useOrSqlSyntax ? "all_tables" : "INFORMATION_SCHEMA.TABLES"));
	}

	private static PreparedStatement getUpsertRowPreparedStatement(Connection conn, String tableName) throws SQLException {
		PreparedStatement ps;
		if (useMySqlSyntax) {
			ps = conn.prepareStatement(Utils.formatMessage(
					"INSERT INTO {0} VALUES (?,?,?,?,NULL) ON DUPLICATE KEY UPDATE {1}=?,{2}=?,{3}=?,{4}=NULL",
					tableName, Config._TYPE, Config._CREATORID, JSON_FIELD_NAME, JSON_UPDATES_FIELD_NAME));
		} else if (usePGSqlSyntax || useLiSqlSyntax) {
			ps = conn.prepareStatement(Utils.formatMessage(
					"INSERT INTO {0} VALUES (?,?,?,?,NULL) ON CONFLICT ({1}) DO UPDATE SET {2}=?,{3}=?,{4}=?,{5}=NULL",
					tableName, Config._ID, Config._TYPE, Config._CREATORID, JSON_FIELD_NAME, JSON_UPDATES_FIELD_NAME));
		} else if (useMSSqlSyntax) {
			// UPSERT snippet taken from https://samsaffron.com/blog/archive/2007/04/04/14.aspx
			ps = conn.prepareStatement(Utils.formatMessage("begin tran\n"
					+ "if exists (select * from {0} with (updlock,serializable) where {1} = ?)\n"
					+ "begin\n"
					+ "   update {0} set {2}=?,{3}=?,{4}=?,{5}=NULL where {1} = ?\n"
					+ "end\n"
					+ "else\n"
					+ "begin\n"
					+ "   insert {0} ({1},{2},{3},{4},{5}) values (?,?,?,?,NULL)\n"
					+ "end\n"
					+ "commit tran", tableName, Config._ID, Config._TYPE, Config._CREATORID,
					JSON_FIELD_NAME, JSON_UPDATES_FIELD_NAME));
		} else if (useOrSqlSyntax) {
			// https://stackoverflow.com/questions/4015199/oracle-sql-update-if-exists-else-insert
			ps = conn.prepareStatement(Utils.formatMessage("MERGE INTO {0} d USING "
					+ "(SELECT ? {1}, ? {2}, ? {3}, ? {4}, NULL {5} FROM dual) s "
					+ "ON (d.{1} = s.{1}) WHEN MATCHED THEN "
					+ "UPDATE SET d.{2} = s.{2}, d.{3} = s.{3}, d.{4} = s.{4}, d.{5} = s.{5} WHEN NOT MATCHED THEN "
					+ "INSERT (d.{1}, d.{2}, d.{3}, d.{4}, d.{5}) VALUES (s.{1}, s.{2}, s.{3}, s.{4}, s.{5})",
					tableName, Config._ID, Config._TYPE, Config._CREATORID, JSON_FIELD_NAME, JSON_UPDATES_FIELD_NAME));
		} else {
			ps = conn.prepareStatement(Utils.formatMessage("MERGE INTO {0} VALUES (?,?,?,?,NULL)", tableName));
		}
		return ps;
	}

	private static PreparedStatement getReadPagePreparedStatement(Connection conn, String tableName) throws SQLException {
		PreparedStatement ps;
		if (useMSSqlSyntax) {
			ps = conn.prepareStatement(Utils.formatMessage("SELECT TOP (?) * FROM (SELECT {0},{1},ROW_NUMBER() OVER "
					+ "(ORDER BY ID ASC) AS RN FROM {2}) AS X WHERE RN > ?",
					JSON_FIELD_NAME, JSON_UPDATES_FIELD_NAME, tableName));
		} else if (useOrSqlSyntax) {
			ps = conn.prepareStatement(Utils.formatMessage("SELECT {0},{1} FROM {2} OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
					JSON_FIELD_NAME, JSON_UPDATES_FIELD_NAME, tableName));
		} else {
			ps = conn.prepareStatement(Utils.formatMessage("SELECT {0},{1} FROM {2} LIMIT ? OFFSET ?",
					JSON_FIELD_NAME, JSON_UPDATES_FIELD_NAME, tableName));
		}
		return ps;
	}

	private static void throwIfNecessary(Throwable t) {
		if (t != null && Config.getConfigBoolean("fail_on_write_errors", true)) {
			throw new RuntimeException("DAO write operation failed!", t);
		}
	}

	private static String logSqlError(Exception e) {
		if (e == null || !SQLException.class.isAssignableFrom(e.getClass())) {
			return "";
		}
		SQLException sqlException = (SQLException) e;
		return " [" + e.getMessage() + " (Error Code: " + sqlException.getErrorCode() +
				", SQLState: " + sqlException.getSQLState() + ")]";
	}
}
