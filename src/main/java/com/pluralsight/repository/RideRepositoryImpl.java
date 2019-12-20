package com.pluralsight.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import com.pluralsight.model.Ride;
import com.pluralsight.repository.util.RideRowMapper;

@Repository("rideRepository")
public class RideRepositoryImpl implements RideRepository {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Override
	public Ride createRide(Ride ride) {
		//jdbcTemplate.update("insert into ride (name, duration) values (?, ?)", ride.getName(), ride.getDuration());
		
		// Keyholder is what stores the key that gets retrieved from our database while we do an insert. Use GeneratedKeyHolder because we have an auto
		// incrementing field. 
		/*
		 * KeyHolder keyHolder = new GeneratedKeyHolder(); jdbcTemplate.update(new
		 * PreparedStatementCreator() {
		 * 
		 * @Override public PreparedStatement createPreparedStatement(Connection con)
		 * throws SQLException { PreparedStatement ps =
		 * con.prepareStatement("insert into ride (name, duration) values (?, ?)", new
		 * String [] {"id"}); ps.setString(1, ride.getName()); ps.setInt(2,
		 * ride.getDuration()); return ps; } }, keyHolder); // This tells the insert
		 * statement to store that generated id back in this keyHolder. Number id =
		 * keyHolder.getKey();
		 */
		SimpleJdbcInsert insert = new SimpleJdbcInsert(jdbcTemplate);
	
		insert.setGeneratedKeyName("id");
		
		Map<String, Object> data = new HashMap<>();
		data.put("name", ride.getName());
		data.put("duration", ride.getDuration());
		
		List<String> columns = new ArrayList<>();
		columns.add("name");
		columns.add("duration");
		
		insert.setTableName("ride");
		insert.setColumnNames(columns);
		Number id = insert.executeAndReturnKey(data);
		
		return getRide(id.intValue());
	}
	
	@Override // @Override Exposes this method to other tiers.
	public Ride getRide(Integer id) {
		Ride ride = jdbcTemplate.queryForObject("select * from ride where id = ?", new RideRowMapper(), id);
		return ride;
	}
	
	// Since this is a template method pattern, all we have to define is mapRow method, which takes these items from our result set (rs) & enables us to 
	// store them in a ride object. Behind the scenes, it adds this to an array list & returns that out of our query to our list of rides, & then we return
	// the rides array list at the bottom of the method.
	@Override
	public List<Ride> getRides() {
		List<Ride> rides = jdbcTemplate.query("select * from ride", new RideRowMapper());
		return rides;
	}
	
	@Override
	public Ride updateRide(Ride ride) {
		jdbcTemplate.update("update ride set name = ?, duration = ? where id = ?", ride.getName(), ride.getDuration(), ride.getId());
		return ride;
	}
	
	// batchUpate can update rows & insert rows. E.g., to insert 20 rows at once, you can use the insert statement. The batchUpdate will execute
	// whatever you have based off that ArrayList of Ojbects that you're passing into it & execute it for each row. The order of those elements does 
	// matter, as it does look for the question marks in a specific order. 
	@Override
	public void updateRides(List<Object[]> pairs) {
		jdbcTemplate.batchUpdate("update ride set ride_date = ? where id = ?", pairs);
	}
	
	@Override
	public void deleteRide(Integer id) {
		// jdbcTemplate.update("delete from ride where id = ?", id);
		NamedParameterJdbcTemplate namedTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
		
		Map<String, Object> paramMap = new HashMap<>();
		paramMap.put("id", id);
		
		namedTemplate.update("delete from ride where id = :id", paramMap);
	}
}
