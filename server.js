const express = require('express');
const mysql = require('mysql2/promise');
const bodyParser = require('body-parser');
const cors = require('cors'); // ADD THIS
const cron = require('node-cron');

const app = express();
const PORT = 3000;

// Middleware
app.use(cors()); // ADD THIS - Enable CORS for all routes
app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.urlencoded({ extended: true, limit: '50mb' }));

// MySQL Connection Pool
const pool = mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: '', // Change this to your MySQL password
    database: 'mighty_party_turf_war',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Test database connection
pool.getConnection()
    .then(connection => {
        console.log('✓ Database connected successfully');
        connection.release();
    })
    .catch(err => {
        console.error('✗ Database connection failed:', err.message);
    });

// Helper function to determine round number and snipe time
function getRoundInfo(timestamp) {
    const date = new Date(timestamp);
    const hours = date.getUTCHours();
    const minutes = date.getUTCMinutes();
    
    let roundNumber;
    let isSnipeTime = false;
    
    // Determine round based on UTC time
    // R1: 00:00:01 - 05:59:59 (snipe: 05:45:00 - 05:59:59)
    // R2: 06:00:00 - 17:59:59 (snipe: 17:45:00 - 17:59:59)
    // R3: 18:00:00 - 23:59:59 (snipe: 23:45:00 - 23:59:59)
    
    if (hours >= 0 && hours < 6) {
        roundNumber = 1;
        isSnipeTime = (hours === 5 && minutes >= 45);
    } else if (hours >= 6 && hours < 18) {
        roundNumber = 2;
        isSnipeTime = (hours === 17 && minutes >= 45);
    } else {
        roundNumber = 3;
        isSnipeTime = (hours === 23 && minutes >= 45);
    }
    
    return { roundNumber, isSnipeTime };
}


// Convert ISO string to MySQL datetime format
function toMySQLDateTime(isoString) {
    if (!isoString) return null;
    return new Date(isoString).toISOString().slice(0, 19).replace('T', ' ');
}

app.post('/api/turf-war/snapshot', async (req, res) => {
    let connection;
    
    try {
        const data = req.body;
        
        if (!data || !data.result) {
            return res.status(400).json({ 
                success: false, 
                error: 'Invalid data format. Expected result object.' 
            });
        }

        const { guild, members } = data.result;
        
        if (!guild || !members) {
            return res.status(400).json({ 
                success: false, 
                error: 'Missing guild or members data' 
            });
        }

        connection = await pool.getConnection();
        await connection.beginTransaction();

        const now = new Date();
        const { roundNumber, isSnipeTime, utc5Date } = getRoundInfo(now);
        
        const snapshotDatetime = utc5Date.toISOString().slice(0, 19).replace('T', ' ');
        const snapshotDate = utc5Date.toISOString().slice(0, 10);
        const snapshotTime = utc5Date.toISOString().slice(11, 19);

        // Calculate DEPLOYED power (sum of all members' summary_power - affected by elixir)
        const totalDeployedPower = members.reduce((sum, m) => sum + (m.summary_power || 0), 0);
        const totalSpentElixir = members.reduce((sum, m) => sum + (m.spent_elixir || 0), 0);

        // ============================================
        // 1. UPSERT GUILD DATA
        // ============================================
        await connection.execute(`
            INSERT INTO guilds (
                id, name, slogan, \`rank\`, icon, created_on, is_qa_guild,
                min_might, influence, auto_accept_requests, internal_message,
                members_count, average_power, summary_power, max_summary_power,
                pinned_message, game_server, current_members, current_officers,
                request_id, invite_id, place, old_place, rating_points
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                slogan = VALUES(slogan),
                \`rank\` = VALUES(\`rank\`),
                icon = VALUES(icon),
                is_qa_guild = VALUES(is_qa_guild),
                min_might = VALUES(min_might),
                influence = VALUES(influence),
                auto_accept_requests = VALUES(auto_accept_requests),
                internal_message = VALUES(internal_message),
                members_count = VALUES(members_count),
                average_power = VALUES(average_power),
                summary_power = VALUES(summary_power),
                max_summary_power = VALUES(max_summary_power),
                pinned_message = VALUES(pinned_message),
                game_server = VALUES(game_server),
                current_members = VALUES(current_members),
                current_officers = VALUES(current_officers),
                request_id = VALUES(request_id),
                invite_id = VALUES(invite_id),
                place = VALUES(place),
                old_place = VALUES(old_place),
                rating_points = VALUES(rating_points)
        `, [
            guild.id, guild.name, guild.slogan, guild.rank, guild.icon,
            toMySQLDateTime(guild.created_on), guild.is_qa_guild, guild.min_might,
            guild.influence, guild.auto_accept_requests, guild.internal_message,
            guild.members_count, guild.average_power, guild.summary_power,
            guild.max_summary_power, guild.pinned_message, guild.game_server,
            guild.current_members, guild.current_officers, guild.request_id,
            guild.invite_id, guild.place, guild.old_place, guild.rating_points
        ]);

        // ============================================
        // 2. INSERT GUILD SNAPSHOT
        // ============================================
        await connection.execute(`
            INSERT INTO guild_snapshots (
                guild_id, snapshot_date, snapshot_time, snapshot_datetime,
                round_number, is_snipe_time, total_deployed_power, total_spent_elixir,
                members_count, average_power, guild_might, \`rank\`, place, rating_points
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                total_deployed_power = VALUES(total_deployed_power),
                total_spent_elixir = VALUES(total_spent_elixir),
                members_count = VALUES(members_count),
                average_power = VALUES(average_power),
                guild_might = VALUES(guild_might),
                \`rank\` = VALUES(\`rank\`),
                place = VALUES(place),
                rating_points = VALUES(rating_points)
        `, [
            guild.id, snapshotDate, snapshotTime, snapshotDatetime,
            roundNumber, isSnipeTime, totalDeployedPower, totalSpentElixir,
            guild.members_count, guild.average_power, guild.summary_power,
            guild.rank, guild.place, guild.rating_points
        ]);

        // ============================================
        // 3. PROCESS EACH MEMBER (same as before)
        // ============================================
        let membersProcessed = 0;
        let snapshotsCreated = 0;

        for (const member of members) {
            // Upsert player data
            await connection.execute(`
                INSERT INTO players (
                    profile_id, guild_id, name, prefix, country, frame_id, medal_id,
                    timezone, medal_value, chosen_language, country_is_static,
                    joined_on, role, was_guild_master, locked_gw_till, locked_gs_till,
                    locked_regatta_till, locked_gf_till, created_on, type,
                    remaining_power, guild_might, fame, game_server, last_visit,
                    warlord_id, warlord_promote
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    guild_id = VALUES(guild_id),
                    name = VALUES(name),
                    prefix = VALUES(prefix),
                    country = VALUES(country),
                    frame_id = VALUES(frame_id),
                    medal_id = VALUES(medal_id),
                    timezone = VALUES(timezone),
                    medal_value = VALUES(medal_value),
                    chosen_language = VALUES(chosen_language),
                    country_is_static = VALUES(country_is_static),
                    role = VALUES(role),
                    locked_gw_till = VALUES(locked_gw_till),
                    locked_gs_till = VALUES(locked_gs_till),
                    locked_regatta_till = VALUES(locked_regatta_till),
                    locked_gf_till = VALUES(locked_gf_till),
                    remaining_power = VALUES(remaining_power),
                    guild_might = VALUES(guild_might),
                    fame = VALUES(fame),
                    last_visit = VALUES(last_visit),
                    warlord_id = VALUES(warlord_id),
                    warlord_promote = VALUES(warlord_promote)
            `, [
                member.profile_id, guild.id,
                member.NameBit?.Name || '', member.NameBit?.Prefix || '',
                member.NameBit?.Country || '', member.NameBit?.frameId || 0,
                member.NameBit?.medalId || 0, member.NameBit?.TimeZone || '',
                member.NameBit?.medalValue || 0, member.NameBit?.ChoosenLanguage || '',
                member.NameBit?.CountryIsStatic || false,
                toMySQLDateTime(member.joined_on), member.role,
                member.was_guild_master, toMySQLDateTime(member.locked_gw_till),
                toMySQLDateTime(member.locked_gs_till), toMySQLDateTime(member.locked_regatta_till),
                toMySQLDateTime(member.locked_gf_till), toMySQLDateTime(member.created_on),
                member.type, member.remaining_power, member.guild_might,
                member.fame, member.game_server, toMySQLDateTime(member.last_visit),
                member.warlord_id, member.warlord_promote
            ]);

            membersProcessed++;

            // Insert turf war snapshot
            await connection.execute(`
                INSERT INTO turf_war_snapshots (
                    profile_id, guild_id, snapshot_date, snapshot_time, snapshot_datetime,
                    round_number, is_snipe_time, summary_power, spent_elixir,
                    remaining_power, guild_might
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    summary_power = VALUES(summary_power),
                    spent_elixir = VALUES(spent_elixir),
                    remaining_power = VALUES(remaining_power),
                    guild_might = VALUES(guild_might)
            `, [
                member.profile_id, guild.id, snapshotDate, snapshotTime, snapshotDatetime,
                roundNumber, isSnipeTime, member.summary_power || 0,
                member.spent_elixir || 0, member.remaining_power, member.guild_might
            ]);

            snapshotsCreated++;
        }

        await connection.commit();

        res.json({
            success: true,
            message: 'Data saved successfully',
            details: {
                guild: guild.name,
                guild_id: guild.id,
                guild_might: guild.summary_power,
                total_deployed_power: totalDeployedPower,
                members_processed: membersProcessed,
                snapshots_created: snapshotsCreated,
                snapshot_datetime: snapshotDatetime,
                round_number: roundNumber,
                is_snipe_time: isSnipeTime
            }
        });

    } catch (error) {
        if (connection) {
            await connection.rollback();
        }
        
        console.error('Error processing data:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    } finally {
        if (connection) {
            connection.release();
        }
    }
});


// Health check endpoint
app.get('/api/health', async (req, res) => {
    try {
        const connection = await pool.getConnection();
        await connection.execute('SELECT 1');
        connection.release();
        
        res.json({ 
            status: 'healthy', 
            database: 'connected',
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.status(500).json({ 
            status: 'unhealthy', 
            database: 'disconnected',
            error: error.message 
        });
    }
});
app.get('/api/dashboard/guild/:guildId/hourly-pattern', async (req, res) => {
    try {
        const { guildId } = req.params;
        const connection = await pool.getConnection();
        
        // Get today in UTC+5
        const [utc5Row] = await connection.execute(`
            SELECT DATE(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 5 HOUR)) AS today_utc5
        `);
        const todayUtc5 = utc5Row[0].today_utc5;
        
        // Get today's data
        const [todayDataSimple] = await connection.execute(`
            SELECT 
                HOUR(snapshot_datetime) as hour,
                MINUTE(snapshot_datetime) as minute,
                MAX(total_deployed_power) as deployed_power
            FROM guild_snapshots
            WHERE guild_id = ?
                AND snapshot_date = ?
            GROUP BY HOUR(snapshot_datetime), MINUTE(snapshot_datetime)
            ORDER BY HOUR(snapshot_datetime), MINUTE(snapshot_datetime)
        `, [guildId, todayUtc5]);
        
        // Get the maximum day ever
        const [maxDayData] = await connection.execute(`
            SELECT 
                HOUR(gs.snapshot_datetime) as hour,
                MINUTE(gs.snapshot_datetime) as minute,
                gs.total_deployed_power as deployed_power
            FROM guild_snapshots gs
            WHERE gs.guild_id = ?
                AND gs.snapshot_date = (
                    SELECT snapshot_date
                    FROM guild_snapshots
                    WHERE guild_id = ?
                        AND HOUR(snapshot_datetime) = 23
                        AND total_deployed_power > 0
                    GROUP BY snapshot_date
                    ORDER BY MAX(total_deployed_power) DESC
                    LIMIT 1
                )
            ORDER BY gs.snapshot_datetime
        `, [guildId, guildId]);
        
        // Get the minimum day ever
        const [minDayData] = await connection.execute(`
            SELECT 
                HOUR(gs.snapshot_datetime) as hour,
                MINUTE(gs.snapshot_datetime) as minute,
                gs.total_deployed_power as deployed_power
            FROM guild_snapshots gs
            WHERE gs.guild_id = ?
                AND gs.snapshot_date = (
                    SELECT snapshot_date
                    FROM guild_snapshots
                    WHERE guild_id = ?
                        AND HOUR(snapshot_datetime) = 23
                        AND total_deployed_power > 0
                    GROUP BY snapshot_date
                    ORDER BY MAX(total_deployed_power) ASC
                    LIMIT 1
                )
            ORDER BY gs.snapshot_datetime
        `, [guildId, guildId]);
        
        // Get average day pattern (last 30 days, excluding today)
        const [avgData] = await connection.execute(`
            SELECT 
                HOUR(snapshot_datetime) as hour,
                MINUTE(snapshot_datetime) as minute,
                AVG(total_deployed_power) as deployed_power
            FROM guild_snapshots
            WHERE guild_id = ?
                AND snapshot_date >= DATE_SUB(?, INTERVAL 30 DAY)
                AND snapshot_date < ?
                AND total_deployed_power > 0
            GROUP BY HOUR(snapshot_datetime), MINUTE(snapshot_datetime)
            ORDER BY HOUR(snapshot_datetime), MINUTE(snapshot_datetime)
        `, [guildId, todayUtc5, todayUtc5]);
        
        // Transform data to display format (remap to hour windows)
        function transformToDisplayFormat(data) {
            const result = [];
            
            data.forEach(row => {
                const hour = parseInt(row.hour);
                const minute = parseInt(row.minute);
                const power = parseFloat(row.deployed_power);
                
                if (minute === 55 && [5, 17, 23].includes(hour)) {
                    // This is :55 snipe data - add as round end time
                    if (hour === 23) {
                        result.push({ hour: 24, minute: 0, deployed_power: power });
                    } else {
                        result.push({ hour: hour + 1, minute: 0, deployed_power: power });
                    }
                } else if (minute === 45) {
                    // Keep all :45 times as-is (including 5:45, 17:45, 23:45)
                    result.push({ hour: hour, minute: 45, deployed_power: power });
                } else if (minute > 45) {
                    // Other times after :45 (but not :55 on snipe hours) - map to next hour
                    result.push({ hour: (hour + 1) % 24, minute: 0, deployed_power: power });
                } else {
                    // Before :45 - map to current hour
                    result.push({ hour: hour, minute: 0, deployed_power: power });
                }
            });
            
            // Group by time label, keep MAX power for duplicates
            const uniqueMap = {};
            result.forEach(item => {
                const key = `${item.hour}:${String(item.minute).padStart(2, '0')}`;
                if (!uniqueMap[key] || item.deployed_power > uniqueMap[key].deployed_power) {
                    uniqueMap[key] = item;
                }
            });
            
            // Convert back to array and sort
            return Object.values(uniqueMap).sort((a, b) => {
                // Handle hour 24 specially (should come last)
                const hourA = a.hour === 24 ? 24 : a.hour;
                const hourB = b.hour === 24 ? 24 : b.hour;
                if (hourA !== hourB) return hourA - hourB;
                return a.minute - b.minute;
            });
        }



        
        connection.release();
        
        res.json({
            success: true,
            today: transformToDisplayFormat(todayDataSimple),
            max_day: transformToDisplayFormat(maxDayData),
            min_day: transformToDisplayFormat(minDayData),
            average_day: transformToDisplayFormat(avgData)
        });
        
    } catch (error) {
        console.error('Error fetching hourly pattern:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});



// ============================================
// STEP 4: DASHBOARD DATA RETRIEVAL FUNCTIONS
// ============================================

// Get all guilds with their latest snapshot data and comparisons
app.get('/api/dashboard/guilds', async (req, res) => {
    try {
        const connection = await pool.getConnection();
        
        // Get latest snapshot for each guild (today's data)
        const [guilds] = await connection.execute(`
            SELECT 
                g.id,
                g.name,
                g.\`rank\`,
                g.place,
                g.members_count,
                g.team as team,
                gs_latest.total_summary_power as today_power,
                gs_latest.total_spent_elixir as today_elixir,
                gs_latest.snapshot_datetime as last_update,
                gs_latest.round_number as current_round,
                AVG(gs_30days.total_summary_power) as avg_power_30days,
                AVG(gs_30days.total_spent_elixir) as avg_elixir_30days
            FROM guilds g
            LEFT JOIN guild_snapshots gs_latest ON g.id = gs_latest.guild_id 
                AND gs_latest.snapshot_datetime = (
                    SELECT MAX(snapshot_datetime) 
                    FROM guild_snapshots 
                    WHERE guild_id = g.id
                )
            LEFT JOIN guild_snapshots gs_30days ON g.id = gs_30days.guild_id
                AND gs_30days.snapshot_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                AND gs_30days.round_number = 3
            GROUP BY g.id, g.name, g.\`rank\`, g.place, g.members_count, 
                     gs_latest.total_summary_power, gs_latest.total_spent_elixir,
                     gs_latest.snapshot_datetime, gs_latest.round_number
            ORDER BY g.\`rank\` ASC
        `);
        
        connection.release();
        
        res.json({
            success: true,
            guilds: guilds.map(guild => ({
                ...guild,
                power_vs_average: guild.avg_power_30days 
                    ? ((guild.today_power - guild.avg_power_30days) / guild.avg_power_30days * 100).toFixed(2)
                    : null,
                elixir_vs_average: guild.avg_elixir_30days
                    ? ((guild.today_elixir - guild.avg_elixir_30days) / guild.avg_elixir_30days * 100).toFixed(2)
                    : null
            }))
        });
        
    } catch (error) {
        console.error('Error fetching guilds:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Get detailed guild information
app.get('/api/dashboard/guild/:guildId', async (req, res) => {
    try {
        const { guildId } = req.params;
        const connection = await pool.getConnection();
        
        // Get today in UTC+5
        const [utc5Row] = await connection.execute(`
            SELECT DATE(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 5 HOUR)) AS today_utc5
        `);
        const todayUtc5 = utc5Row[0].today_utc5;
        
        // Get guild basic info
        const [guildInfo] = await connection.execute(`
            SELECT * FROM guilds WHERE id = ?
        `, [guildId]);
        
        if (guildInfo.length === 0) {
            connection.release();
            return res.status(404).json({ success: false, error: 'Guild not found' });
        }
        
        // Get max placed ever (highest power at end of any day)
        const [maxPlaced] = await connection.execute(`
            SELECT MAX(total_deployed_power) as max_placed
            FROM guild_snapshots
            WHERE guild_id = ?
                AND HOUR(snapshot_datetime) = 23
        `, [guildId]);
        
        // Get today's current power
        const [todayPower] = await connection.execute(`
            SELECT MAX(total_deployed_power) as today_power
            FROM guild_snapshots
            WHERE guild_id = ?
                AND snapshot_date = ?
        `, [guildId, todayUtc5]);
        
        // Get members placed today
        const [membersPlaced] = await connection.execute(`
            SELECT COUNT(DISTINCT profile_id) as placed_count
            FROM turf_war_snapshots
            WHERE guild_id = ?
                AND snapshot_date = ?
                AND summary_power > 0
        `, [guildId, todayUtc5]);
        
        // Get guild might history (last 30 days)
        const [mightHistory] = await connection.execute(`
            SELECT 
                snapshot_date,
                snapshot_time,
                guild_might,
                total_deployed_power,
                total_spent_elixir,
                round_number
            FROM guild_snapshots
            WHERE guild_id = ?
                AND snapshot_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            ORDER BY snapshot_datetime ASC
        `, [guildId]);
        
        // Get average performance by weekday (last 30 days)
        const [weekdayStats] = await connection.execute(`
            SELECT 
                DAYOFWEEK(snapshot_date) as weekday,
                DAYNAME(snapshot_date) as weekday_name,
                AVG(total_deployed_power) as avg_power,
                AVG(total_spent_elixir) as avg_elixir,
                COUNT(DISTINCT snapshot_date) as days_count
            FROM guild_snapshots
            WHERE guild_id = ?
                AND snapshot_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                AND round_number = 3
            GROUP BY DAYOFWEEK(snapshot_date), DAYNAME(snapshot_date)
            ORDER BY weekday
        `, [guildId]);
        
        // Get average performance by round (last 30 days)
        const [roundStats] = await connection.execute(`
            SELECT 
                round_number,
                is_snipe_time,
                AVG(total_deployed_power) as avg_power,
                AVG(total_spent_elixir) as avg_elixir,
                COUNT(*) as sample_count
            FROM guild_snapshots
            WHERE guild_id = ?
                AND snapshot_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            GROUP BY round_number, is_snipe_time
            ORDER BY round_number, is_snipe_time
        `, [guildId]);
        
        // Get members list with today's performance
        const [members] = await connection.execute(`
            SELECT 
                p.profile_id,
                p.name,
                p.country,
                p.role,
                p.guild_might,
                p.fame,
                tw_latest.summary_power as today_power,
                tw_latest.spent_elixir as today_elixir,
                tw_latest.round_number as last_play_round,
                AVG(tw_30days.summary_power) as avg_power_30days,
                AVG(tw_30days.spent_elixir) as avg_elixir_30days
            FROM players p
            LEFT JOIN turf_war_snapshots tw_latest ON p.profile_id = tw_latest.profile_id
                AND tw_latest.snapshot_datetime = (
                    SELECT MAX(snapshot_datetime)
                    FROM turf_war_snapshots
                    WHERE profile_id = p.profile_id
                )
            LEFT JOIN turf_war_snapshots tw_30days ON p.profile_id = tw_30days.profile_id
                AND tw_30days.snapshot_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                AND tw_30days.round_number = 3
            WHERE p.guild_id = ?
            GROUP BY p.profile_id, p.name, p.country, p.role, p.guild_might, p.fame,
                     tw_latest.summary_power, tw_latest.spent_elixir, tw_latest.round_number
            ORDER BY p.guild_might DESC
        `, [guildId]);
        
        connection.release();
        
        const guild = guildInfo[0];
        const maxPlacedEver = maxPlaced[0].max_placed || 0;
        const todayCurrentPower = todayPower[0].today_power || 0;
        const potentialLeft = maxPlacedEver - todayCurrentPower;
        const membersPlacedCount = membersPlaced[0].placed_count || 0;
        
        res.json({
            success: true,
            guild: {
                ...guild,
                max_placed_ever: maxPlacedEver,
                today_power: todayCurrentPower,
                potential_left: potentialLeft,
                members_placed: membersPlacedCount
            },
            might_history: mightHistory,
            weekday_stats: weekdayStats,
            round_stats: roundStats,
            members: members.map(member => ({
                ...member,
                power_vs_average: member.avg_power_30days 
                    ? ((member.today_power - member.avg_power_30days) / member.avg_power_30days * 100).toFixed(2)
                    : null,
                elixir_vs_average: member.avg_elixir_30days
                    ? ((member.today_elixir - member.avg_elixir_30days) / member.avg_elixir_30days * 100).toFixed(2)
                    : null
            }))
        });
        
    } catch (error) {
        console.error('Error fetching guild details:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});


// Get player details and statistics
app.get('/api/dashboard/player/:profileId', async (req, res) => {
    try {
        const { profileId } = req.params;
        const connection = await pool.getConnection();
        
        // Get today in UTC+5
        const [utc5Row] = await connection.execute(`
            SELECT DATE(CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', '+05:00')) AS today_utc5
        `);
        const todayUtc5 = utc5Row[0].today_utc5;
        
        // Get player basic info with today's power
        const [player] = await connection.execute(`
            SELECT 
                p.profile_id,
                p.name,
                p.guild_might,
                p.fame,
                p.role,
                g.name as guild_name,
                (
                    SELECT MAX(summary_power)
                    FROM turf_war_snapshots
                    WHERE profile_id = p.profile_id
                        AND snapshot_date = ?
                ) as today_power
            FROM players p
            LEFT JOIN guilds g ON p.guild_id = g.id
            WHERE p.profile_id = ?
        `, [todayUtc5, profileId]);
        
        if (player.length === 0) {
            connection.release();
            return res.status(404).json({ success: false, error: 'Player not found' });
        }
        
        // Get weekday statistics (last 30 days) - use max power per day
        const [weekdayStats] = await connection.execute(`
            SELECT 
                DAYNAME(snapshot_date) as weekday_name,
                DAYOFWEEK(snapshot_date) as weekday_num,
                AVG(max_power) as avg_power,
                AVG(max_elixir) as avg_elixir,
                COUNT(DISTINCT snapshot_date) as days_played
            FROM (
                SELECT 
                    snapshot_date,
                    MAX(summary_power) as max_power,
                    MAX(spent_elixir) as max_elixir
                FROM turf_war_snapshots
                WHERE profile_id = ?
                    AND snapshot_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                GROUP BY snapshot_date
            ) daily_max
            GROUP BY weekday_name, weekday_num
            ORDER BY weekday_num
        `, [profileId]);
        
        // Get round statistics - only count actual deployments (when power increased from previous snapshot)
        const [roundStats] = await connection.execute(`
            SELECT 
                round_number,
                is_snipe_time,
                AVG(summary_power) as avg_power,
                AVG(spent_elixir) as avg_elixir,
                COUNT(*) as times_played
            FROM (
                SELECT 
                    id,
                    snapshot_date,
                    snapshot_datetime,
                    round_number,
                    is_snipe_time,
                    summary_power,
                    spent_elixir,
                    LAG(summary_power) OVER (ORDER BY snapshot_datetime) as prev_power
                FROM turf_war_snapshots
                WHERE profile_id = ?
                    AND snapshot_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            ) with_prev
            WHERE prev_power IS NULL OR summary_power > prev_power
            GROUP BY round_number, is_snipe_time
            ORDER BY round_number, is_snipe_time
        `, [profileId]);
        
        // Get power proportions by round and snipe/drop (NEW QUERY)
        // Get power proportions by round and snipe/drop - only using :45 and :55 snapshots
        // Get power proportions - AVERAGE of actual deployment amounts
        const [powerProportions] = await connection.execute(`
            SELECT 
                round_number,
                AVG(drop_power) as dropped_power,
                AVG(snipe_power) as sniped_power
            FROM (
                SELECT 
                    snapshot_date,
                    round_number,
                    MAX(CASE WHEN MINUTE(CONVERT_TZ(snapshot_datetime, '+00:00', '+05:00')) = 45 
                        THEN summary_power ELSE 0 END) as drop_power,
                    GREATEST(0, 
                        MAX(CASE WHEN MINUTE(CONVERT_TZ(snapshot_datetime, '+00:00', '+05:00')) = 55 
                            THEN summary_power ELSE 0 END) - 
                        MAX(CASE WHEN MINUTE(CONVERT_TZ(snapshot_datetime, '+00:00', '+05:00')) = 45 
                            THEN summary_power ELSE 0 END)
                    ) as snipe_power
                FROM turf_war_snapshots
                WHERE profile_id = ?
                    AND snapshot_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                    AND MINUTE(CONVERT_TZ(snapshot_datetime, '+00:00', '+05:00')) IN (45, 55)
                GROUP BY snapshot_date, round_number
                HAVING drop_power > 0 OR snipe_power > 0
            ) daily_rounds
            GROUP BY round_number
            ORDER BY round_number
        `, [profileId]);

        
        // Get round preference patterns - only count days where player actually deployed
        const [roundPref] = await connection.execute(`
            SELECT 
                SUM(CASE WHEN rounds_played = 'R1' THEN 1 ELSE 0 END) as r1_only,
                SUM(CASE WHEN rounds_played = 'R2' THEN 1 ELSE 0 END) as r2_only,
                SUM(CASE WHEN rounds_played = 'R3' THEN 1 ELSE 0 END) as r3_only,
                SUM(CASE WHEN rounds_played LIKE '%,%' THEN 1 ELSE 0 END) as multiple_rounds,
                SUM(CASE WHEN has_snipe = 1 THEN 1 ELSE 0 END) as sniper_count
            FROM (
                SELECT 
                    snapshot_date,
                    GROUP_CONCAT(DISTINCT CONCAT('R', round_number) ORDER BY round_number) as rounds_played,
                    MAX(is_snipe_time) as has_snipe
                FROM (
                    SELECT 
                        snapshot_date,
                        snapshot_datetime,
                        round_number,
                        is_snipe_time,
                        summary_power,
                        LAG(summary_power) OVER (ORDER BY snapshot_datetime) as prev_power
                    FROM turf_war_snapshots
                    WHERE profile_id = ?
                        AND snapshot_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                ) with_prev
                WHERE prev_power IS NULL OR summary_power > prev_power
                GROUP BY snapshot_date
            ) daily_activity
        `, [profileId]);
        
        // Get daily playing pattern - separate drops and snipes
        const [playingPattern] = await connection.execute(`
            SELECT 
                snapshot_date,
                MAX(CASE WHEN round_number = 1 AND is_snipe_time = 0 THEN summary_power ELSE 0 END) as r1_drop,
                MAX(CASE WHEN round_number = 1 AND is_snipe_time = 1 THEN summary_power ELSE 0 END) as r1_snipe,
                MAX(CASE WHEN round_number = 2 AND is_snipe_time = 0 THEN summary_power ELSE 0 END) as r2_drop,
                MAX(CASE WHEN round_number = 2 AND is_snipe_time = 1 THEN summary_power ELSE 0 END) as r2_snipe,
                MAX(CASE WHEN round_number = 3 AND is_snipe_time = 0 THEN summary_power ELSE 0 END) as r3_drop,
                MAX(CASE WHEN round_number = 3 AND is_snipe_time = 1 THEN summary_power ELSE 0 END) as r3_snipe
            FROM turf_war_snapshots
            WHERE profile_id = ?
                AND snapshot_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            GROUP BY snapshot_date
            ORDER BY snapshot_date DESC
        `, [profileId]);

        
        connection.release();
        
        res.json({
            success: true,
            player: player[0],
            weekday_stats: weekdayStats,
            round_stats: roundStats,
            power_proportions: powerProportions, // NEW
            round_preference: roundPref[0] || { r1_only: 0, r2_only: 0, r3_only: 0, multiple_rounds: 0, sniper_count: 0 },
            playing_pattern: playingPattern
        });
        
    } catch (error) {
        console.error('Error fetching player data:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});





app.get('/api/dashboard/today-comparison', async (req, res) => {
    try {
        const connection = await pool.getConnection();

        // Get the date parameter or use today in UTC+5
        const targetDate = req.query.date;
        let dateToUse;

        if (targetDate) {
            dateToUse = targetDate;
        } else {
            const [utc5Row] = await connection.execute(`SELECT DATE(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 5 HOUR)) AS today_utc5`);
            dateToUse = utc5Row[0].today_utc5;
        }

        const [comparison] = await connection.execute(`
            SELECT 
                g.id as guild_id,
                g.name as guild_name,
                g.members_count as total_members,
                g.summary_power as guild_might,
                g.team as team,
                gs_today.total_deployed_power as today_deployed_power,
                gs_today.total_spent_elixir as today_elixir,
                gs_today.snapshot_datetime as last_update,
                gs_today.round_number as current_round,
                (
                    SELECT COUNT(DISTINCT tw.profile_id)
                    FROM turf_war_snapshots tw
                    WHERE tw.guild_id = g.id
                      AND tw.snapshot_date = ?
                      AND tw.summary_power > 0
                ) as members_placed,
                (
                    SELECT MAX(total_deployed_power)
                    FROM guild_snapshots
                    WHERE guild_id = g.id
                      AND total_deployed_power IS NOT NULL
                ) as max_deployed_ever,
                gws_hard.avg_power as avg_deployed_hard,
                gws_weak.avg_power as avg_deployed_weak
            FROM guilds g
            LEFT JOIN guild_snapshots gs_today ON g.id = gs_today.guild_id
              AND gs_today.snapshot_date = ?
              AND gs_today.snapshot_datetime = (
                  SELECT MAX(snapshot_datetime)
                  FROM guild_snapshots
                  WHERE guild_id = g.id
                    AND snapshot_date = ?
              )
            LEFT JOIN (
                SELECT 
                    guild_id,
                    AVG(max_power) as avg_power
                FROM (
                    SELECT 
                        guild_id,
                        snapshot_date,
                        MAX(total_deployed_power) as max_power,
                        MAX(total_spent_elixir) as max_elixir
                    FROM guild_snapshots
                    WHERE snapshot_date >= DATE_SUB(?, INTERVAL 30 DAY)
                      AND snapshot_date < ?
                      AND round_number = 3
                    GROUP BY guild_id, snapshot_date
                ) daily
                WHERE max_elixir > 5000
                GROUP BY guild_id
            ) gws_hard ON g.id = gws_hard.guild_id
            LEFT JOIN (
                SELECT 
                    guild_id,
                    AVG(max_power) as avg_power
                FROM (
                    SELECT 
                        guild_id,
                        snapshot_date,
                        MAX(total_deployed_power) as max_power,
                        MAX(total_spent_elixir) as max_elixir
                    FROM guild_snapshots
                    WHERE snapshot_date >= DATE_SUB(?, INTERVAL 30 DAY)
                      AND snapshot_date < ?
                      AND round_number = 3
                    GROUP BY guild_id, snapshot_date
                ) daily
                WHERE max_elixir BETWEEN 1000 AND 5000
                GROUP BY guild_id
            ) gws_weak ON g.id = gws_weak.guild_id
            ORDER BY g.name
        `, [
            dateToUse, // members_placed
            dateToUse, // gs_today join
            dateToUse, // gs_today subquery
            dateToUse, // hard avg start
            dateToUse, // hard avg end
            dateToUse, // weak avg start
            dateToUse  // weak avg end
        ]);

        connection.release();

        res.json({
            success: true,
            date_used: dateToUse,
            comparison: comparison.map(item => ({
                ...item,
                today_deployed_power: item.today_deployed_power || 0,
                today_elixir: item.today_elixir || 0
            }))
        });
    } catch (error) {
        console.error('Error fetching comparison data:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});







// Start server
app.listen(PORT, () => {
    console.log(`\n🚀 Mighty Party Turf War Tracker API`);
    console.log(`   Server running on http://localhost:${PORT}`);
    console.log(`   Health check: http://localhost:${PORT}/api/health`);
    console.log(`   API endpoint: POST http://localhost:${PORT}/api/turf-war/snapshot\n`);
});


// Helper function to get current time in UTC+5
function getCurrentUTC5() {
    const now = new Date();
    // Get UTC time in milliseconds and add 5 hours (5 * 60 * 60 * 1000 ms)
    const utc5Time = new Date(now.getTime() + (5 * 60 * 60 * 1000));
    return utc5Time;
}

// Helper function to determine round number and snipe time based on UTC+5
function getRoundInfo(date = new Date()) {
    // Convert to UTC+5
    const utc5Time = new Date(date.getTime() + (5 * 60 * 60 * 1000));
    
    const hours = utc5Time.getUTCHours();
    const minutes = utc5Time.getUTCMinutes();
    
    let roundNumber;
    let isSnipeTime = false;
    
    // Determine round based on UTC+5 time
    if (hours >= 0 && hours < 6) {
        roundNumber = 1;
        // R1 snipe: 5:46-5:59
        if (hours === 5 && minutes >= 46 && minutes <= 59) {
            isSnipeTime = true;
        }
    } else if (hours >= 6 && hours < 18) {
        roundNumber = 2;
        // R2 snipe: 17:46-17:59
        if (hours === 17 && minutes >= 46 && minutes <= 59) {
            isSnipeTime = true;
        }
    } else {
        roundNumber = 3;
        // R3 snipe: 23:46-23:59
        if (hours === 23 && minutes >= 46 && minutes <= 59) {
            isSnipeTime = true;
        }
    }
    
    return {
        roundNumber,
        isSnipeTime,
        utc5Date: utc5Time
    };
}


// Convert ISO string to MySQL datetime format in UTC+5
function toMySQLDateTime(isoString) {
    if (!isoString) return null;
    const date = new Date(isoString);
    // Shift to UTC+5
    const utc5Date = new Date(date.getTime() + (5 * 60 * 60 * 1000));
    return utc5Date.toISOString().slice(0, 19).replace('T', ' ');
}

// Database table for storing cron jobs
async function createCronJobsTable() {
    const connection = await pool.getConnection();
    await connection.execute(`
        CREATE TABLE IF NOT EXISTS cron_jobs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            guild_id INT NOT NULL,
            guild_name VARCHAR(255),
            url TEXT NOT NULL,
            method VARCHAR(10) DEFAULT 'POST',
            headers JSON NOT NULL,
            body TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            last_success DATETIME,
            last_failure DATETIME,
            failure_count INT DEFAULT 0,
            failure_message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_guild (guild_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
    connection.release();
}

// Call this on startup
createCronJobsTable().catch(console.error);

// API endpoint to save intercepted requests
app.post('/api/cron/save-request', async (req, res) => {
    try {
        const { url, method, headers, body, timestamp } = req.body;
        
        if (!url || !headers || !body) {
            return res.status(400).json({
                success: false,
                error: 'Missing required fields: url, headers, body'
            });
        }
        
        // FIX: Ensure body is always a string
        let bodyString;
        if (typeof body === 'string') {
            bodyString = body;
        } else if (typeof body === 'object') {
            bodyString = JSON.stringify(body);
        } else {
            return res.status(400).json({
                success: false,
                error: 'Invalid body format'
            });
        }
        
        // Parse body to get guild_id
        const bodyData = JSON.parse(bodyString);
        const guildId = bodyData.guild_id;
        
        if (!guildId) {
            return res.status(400).json({
                success: false,
                error: 'guild_id not found in request body'
            });
        }
        
        const connection = await pool.getConnection();
        
        // Check if guild exists in our database
        const [guildInfo] = await connection.execute(
            'SELECT name FROM guilds WHERE id = ?',
            [guildId]
        );
        
        const guildName = guildInfo.length > 0 ? guildInfo[0].name : null;
        
        // Save or update the cron job - body is now guaranteed to be a string
        await connection.execute(`
            INSERT INTO cron_jobs (guild_id, guild_name, url, method, headers, body, is_active)
            VALUES (?, ?, ?, ?, ?, ?, TRUE)
            ON DUPLICATE KEY UPDATE
                url = VALUES(url),
                method = VALUES(method),
                headers = VALUES(headers),
                body = VALUES(body),
                guild_name = VALUES(guild_name),
                is_active = TRUE,
                failure_count = 0,
                failure_message = NULL
        `, [
            guildId,
            guildName,
            url,
            method || 'POST',
            JSON.stringify(headers),
            bodyString  // Use bodyString here
        ]);
        
        // Get the job we just created/updated
        const [savedJob] = await connection.execute(
            'SELECT * FROM cron_jobs WHERE guild_id = ?',
            [guildId]
        );
        
        connection.release();
        
        // Restart cron scheduler with new job
        await initializeCronJobs();
        
        // Execute the job immediately
        console.log(`[Cron] Executing job immediately for guild ${guildId}`);
        await executeCronJob(savedJob[0]);
        
        res.json({
            success: true,
            message: 'Request saved, cron job created, and executed immediately',
            guild_id: guildId,
            guild_name: guildName
        });
        
    } catch (error) {
        console.error('Error saving request:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});



// Store active cron tasks
const activeCronTasks = new Map();

// Initialize cron jobs from database
async function initializeCronJobs() {
    try {
        // Stop all existing cron jobs
        activeCronTasks.forEach(task => task.stop());
        activeCronTasks.clear();
        
        const connection = await pool.getConnection();
        const [jobs] = await connection.execute(
            'SELECT * FROM cron_jobs WHERE is_active = TRUE'
        );
        connection.release();
        
        console.log(`[Cron] Initializing ${jobs.length} cron jobs`);
        
        jobs.forEach((job, index) => {
            const delaySeconds = index * 3; // Space out by 3 seconds
            
            // Schedule 1: Every hour at minute 45
            const task45 = cron.schedule('45 * * * *', async () => {
                setTimeout(async () => {
                    console.log(`[Cron] Minute 45 execution for guild ${job.guild_id}`);
                    await executeCronJob(job);
                }, delaySeconds * 1000);
            }, {
                scheduled: true,
                timezone: "UTC"
            });
            
            // Schedule 2: UTC hours 0, 12, 18 at minute 55 (snipe times in UTC+5: 5:55, 17:55, 23:55)
            const task55 = cron.schedule('55 0,12,18 * * *', async () => {
                setTimeout(async () => {
                    console.log(`[Cron] SNIPE TIME execution for guild ${job.guild_id}`);
                    await executeCronJob(job);
                }, delaySeconds * 1000);
            }, {
                scheduled: true,
                timezone: "UTC"
            });
            
            // Store both tasks for this job
            activeCronTasks.set(`${job.id}_45`, task45);
            activeCronTasks.set(`${job.id}_55`, task55);
            
            console.log(`[Cron] Scheduled job for guild ${job.guild_id} (${job.guild_name}) - every hour at :45 and snipe times (0:55, 12:55, 18:55 UTC = 5:55, 17:55, 23:55 UTC+5) with ${delaySeconds}s delay`);
        });
        
        console.log(`[Cron] Total scheduled tasks: ${activeCronTasks.size} (${jobs.length} guilds × 2 schedules)`);
        
    } catch (error) {
        console.error('[Cron] Error initializing cron jobs:', error);
    }
}


// Execute a single cron job
async function executeCronJob(job) {
    console.log(`[Cron] Executing job for guild ${job.guild_id} (${job.guild_name})`);
    
    let connection;
    
    try {
        // FIX: headers is already an object (MySQL JSON type auto-parses)
        const headers = typeof job.headers === 'string' ? JSON.parse(job.headers) : job.headers;
        const body = job.body;
        
        console.log(`[Cron] Making request to ${job.url}`);
        console.log(`[Cron] Body type: ${typeof body}`);
        
        // Make the request to the game API
        const response = await fetch(job.url, {
            method: job.method,
            headers: headers,
            body: body
        });
        
        if (!response.ok) {
            throw new Error(`Game API returned ${response.status}: ${response.statusText}`);
        }
        
        const gameData = await response.json();
        
        // Validate response structure
        if (!gameData.result || !gameData.result.guild) {
            throw new Error('Invalid response structure from game API');
        }
        
        console.log(`[Cron] Received data for guild: ${gameData.result.guild.name}`);
        
        // Process the data through our existing endpoint
        const processResponse = await fetch('http://localhost:3000/api/turf-war/snapshot', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(gameData)
        });
        
        if (!processResponse.ok) {
            const errorText = await processResponse.text();
            throw new Error(`Failed to process data: ${processResponse.status} - ${errorText}`);
        }
        
        // Update success status
        connection = await pool.getConnection();
        await connection.execute(`
            UPDATE cron_jobs 
            SET last_success = NOW(), failure_count = 0, failure_message = NULL
            WHERE id = ?
        `, [job.id]);
        connection.release();
        
        console.log(`[Cron] ✓ Successfully executed job for guild ${job.guild_id}`);
        
    } catch (error) {
        console.error(`[Cron] ✗ Error executing job for guild ${job.guild_id}:`, error.message);
        
        try {
            connection = await pool.getConnection();
            await connection.execute(`
                UPDATE cron_jobs 
                SET last_failure = NOW(), 
                    failure_count = failure_count + 1,
                    failure_message = ?,
                    is_active = CASE 
                        WHEN failure_count >= 2 THEN FALSE 
                        ELSE TRUE 
                    END
                WHERE id = ?
            `, [error.message, job.id]);
            connection.release();
            
            // If this job has failed 3 times, stop the cron task
            if (job.failure_count >= 2) {
                const task = activeCronTasks.get(job.id);
                if (task) {
                    task.stop();
                    activeCronTasks.delete(job.id);
                    console.log(`[Cron] Stopped job for guild ${job.guild_id} after multiple failures`);
                }
            }
            
        } catch (dbError) {
            console.error('[Cron] Error updating failure status:', dbError);
        }
    }
}



// Get cron job status
app.get('/api/cron/status', async (req, res) => {
    try {
        const connection = await pool.getConnection();
        const [jobs] = await connection.execute(`
            SELECT 
                id,
                guild_id,
                guild_name,
                is_active,
                last_success,
                last_failure,
                failure_count,
                failure_message,
                created_at,
                updated_at
            FROM cron_jobs
            ORDER BY guild_name
        `);
        connection.release();
        
        res.json({
            success: true,
            jobs: jobs,
            active_count: jobs.filter(j => j.is_active).length,
            failed_count: jobs.filter(j => !j.is_active).length
        });
        
    } catch (error) {
        console.error('Error fetching cron status:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Manually reactivate a failed cron job
app.post('/api/cron/reactivate/:jobId', async (req, res) => {
    try {
        const { jobId } = req.params;
        
        const connection = await pool.getConnection();
        await connection.execute(`
            UPDATE cron_jobs 
            SET is_active = TRUE, failure_count = 0, failure_message = NULL
            WHERE id = ?
        `, [jobId]);
        connection.release();
        
        // Reinitialize cron jobs
        await initializeCronJobs();
        
        res.json({
            success: true,
            message: 'Cron job reactivated'
        });
        
    } catch (error) {
        console.error('Error reactivating cron job:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Add this function to calculate weekly performance
async function calculateWeeklyPerformance(connection, guildId, weekStartDate) {
    // Get week's end date (6 days later)
    const [weekData] = await connection.execute(`
        SELECT 
            DATE_ADD(?, INTERVAL 6 DAY) as week_end,
            SUM(tw.summary_power) as total_power,
            SUM(tw.spent_elixir) as total_elixir,
            COUNT(DISTINCT tw.profile_id) as members_placed,
            (SELECT current_members FROM guilds WHERE id = ?) as total_members
        FROM turf_war_snapshots tw
        WHERE tw.guild_id = ?
            AND tw.snapshot_date >= ?
            AND tw.snapshot_date <= DATE_ADD(?, INTERVAL 6 DAY)
            AND tw.round_number = 3
            AND tw.snapshot_time = (
                SELECT MAX(snapshot_time)
                FROM turf_war_snapshots
                WHERE profile_id = tw.profile_id
                    AND snapshot_date = tw.snapshot_date
                    AND round_number = 3
            )
    `, [weekStartDate, guildId, guildId, weekStartDate, weekStartDate]);

    if (!weekData[0] || !weekData[0].total_members) {
        return null;
    }

    const data = weekData[0];
    const participationRate = (data.members_placed / data.total_members) * 100;
    const avgElixirPerActive = data.members_placed > 0 
        ? Math.round(data.total_elixir / data.members_placed)
        : 0;

    // Classify performance tier
    let tier;
    if (participationRate < 30 || avgElixirPerActive < 1000) {
        tier = 'SKIP';
    } else if (participationRate >= 70 && avgElixirPerActive >= 3000) {
        tier = 'HARD';
    } else {
        tier = 'WEAK';
    }

    // Insert or update weekly performance
    await connection.execute(`
        INSERT INTO guild_weekly_performance (
            guild_id, week_start_date, week_end_date,
            total_power, total_elixir, members_placed, total_members,
            participation_rate, avg_elixir_per_active, performance_tier
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            total_power = VALUES(total_power),
            total_elixir = VALUES(total_elixir),
            members_placed = VALUES(members_placed),
            total_members = VALUES(total_members),
            participation_rate = VALUES(participation_rate),
            avg_elixir_per_active = VALUES(avg_elixir_per_active),
            performance_tier = VALUES(performance_tier)
    `, [
        guildId, weekStartDate, data.week_end,
        data.total_power, data.total_elixir, data.members_placed, data.total_members,
        participationRate.toFixed(2), avgElixirPerActive, tier
    ]);

    return tier;
}

// Add cron job to run this weekly analysis (runs daily at 2 AM)
cron.schedule('0 2 * * *', async () => {
    console.log('[Weekly Analysis] Starting weekly performance calculation...');
    
    try {
        const connection = await pool.getConnection();
        
        // Get all guilds
        const [guilds] = await connection.execute('SELECT id FROM guilds');
        
        // Calculate for last 8 weeks for each guild
        for (const guild of guilds) {
            for (let weeksAgo = 0; weeksAgo < 8; weeksAgo++) {
                const weekStart = new Date();
                weekStart.setDate(weekStart.getDate() - (weeksAgo * 7) - weekStart.getDay()); // Start of week (Sunday)
                const weekStartStr = weekStart.toISOString().split('T')[0];
                
                await calculateWeeklyPerformance(connection, guild.id, weekStartStr);
            }
        }
        
        connection.release();
        console.log('[Weekly Analysis] ✓ Completed weekly performance calculation');
        
    } catch (error) {
        console.error('[Weekly Analysis] Error:', error);
    }
});

// New API endpoint for guild threat assessment
app.get('/api/dashboard/guild/:guildId/threat-assessment', async (req, res) => {
    try {
        const { guildId } = req.params;
        const connection = await pool.getConnection();
        
        // Get max power ever
        const [maxPower] = await connection.execute(`
            SELECT MAX(total_power) as max_power_ever
            FROM guild_weekly_performance
            WHERE guild_id = ?
        `, [guildId]);
        
        // Get average when playing hard (last 30 days)
        const [hardAvg] = await connection.execute(`
            SELECT 
                AVG(total_power) as avg_power_hard,
                AVG(total_elixir) as avg_elixir_hard,
                COUNT(*) as hard_weeks_count
            FROM guild_weekly_performance
            WHERE guild_id = ?
                AND performance_tier = 'HARD'
                AND week_start_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        `, [guildId]);
        
        // Get average when playing weak (last 30 days)
        const [weakAvg] = await connection.execute(`
            SELECT 
                AVG(total_power) as avg_power_weak,
                AVG(total_elixir) as avg_elixir_weak,
                COUNT(*) as weak_weeks_count
            FROM guild_weekly_performance
            WHERE guild_id = ?
                AND performance_tier = 'WEAK'
                AND week_start_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        `, [guildId]);
        
        // Get recent weeks breakdown
        const [recentWeeks] = await connection.execute(`
            SELECT 
                week_start_date,
                week_end_date,
                total_power,
                total_elixir,
                participation_rate,
                avg_elixir_per_active,
                performance_tier
            FROM guild_weekly_performance
            WHERE guild_id = ?
                AND week_start_date >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
            ORDER BY week_start_date DESC
        `, [guildId]);
        
        connection.release();
        
        res.json({
            success: true,
            threat_assessment: {
                max_power_ever: maxPower[0].max_power_ever || 0,
                hard_performance: {
                    avg_power: Math.round(hardAvg[0].avg_power_hard || 0),
                    avg_elixir: Math.round(hardAvg[0].avg_elixir_hard || 0),
                    sample_weeks: hardAvg[0].hard_weeks_count
                },
                weak_performance: {
                    avg_power: Math.round(weakAvg[0].avg_power_weak || 0),
                    avg_elixir: Math.round(weakAvg[0].avg_elixir_weak || 0),
                    sample_weeks: weakAvg[0].weak_weeks_count
                },
                recent_weeks: recentWeeks
            }
        });
        
    } catch (error) {
        console.error('Error fetching threat assessment:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});


// Initialize cron jobs on server start
// Initialize cron jobs on server start - OLD (REMOVE THIS)
// initializeCronJobs();

// Start server - REPLACE WITH THIS
async function startServer() {
    try {
        // Create cron_jobs table first
        await createCronJobsTable();
        console.log('✓ Cron jobs table ready');
        
        // Then initialize cron jobs
        await initializeCronJobs();
        console.log('✓ Cron jobs initialized');
        
        // Start the server
        app.listen(PORT, () => {
            console.log(`\n🚀 Mighty Party Turf War Tracker API`);
            console.log(`   Server running on http://localhost:${PORT}`);
            console.log(`   Health check: http://localhost:${PORT}/api/health`);
            console.log(`   API endpoint: POST http://localhost:${PORT}/api/turf-war/snapshot`);
            console.log(`   Dashboard: http://localhost:${PORT}/dashboard\n`);
        });
        
    } catch (error) {
        console.error('Failed to start server:', error);
        process.exit(1);
    }
}

// Start the server
startServer();