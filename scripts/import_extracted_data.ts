import pg from 'pg';
import fs from 'fs';
import path from 'path';
import { SpanStatusCode } from '@opentelemetry/api';
import { shutdownTracing, tracer } from './otel.js';

interface GarminExportData {
  metadata: {
    start_date: string;
    end_date: string;
    extraction_date: string;
    data_types: string[];
  };
  data: {
    [date: string]: {
      stats?: any;
      steps?: any;
      heart_rate?: any;
      sleep?: any;
      body_composition?: any;
      hydration?: any;
      stress?: any;
      user_summary?: any;
    };
  };
}

interface ActivityData {
  metadata: {
    extraction_date: string;
    activity_count: number;
  };
  activities: Array<{
    summary: any;
    details?: any;
  }>;
}

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} not found in environment`);
  }
  return value;
}

function parseIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return parsed;
}

function resolvePostgresConfig(): pg.ClientConfig {
  return {
    host: process.env.DATABASE_HOST || '127.0.0.1',
    port: parseIntEnv('DATABASE_PORT', 5432),
    database: process.env.DATABASE_NAME || 'garmin_backup',
    user: requiredEnv('DATABASE_USERNAME'),
    password: requiredEnv('DATABASE_PASSWORD'),
  };
}

function resolveExportsDir(): string {
  return process.env.GARMIN_EXPORTS_DIR || './garmin_exports';
}

function parseArgs(argv: string[]): {
  dailyFiles: string[];
  activityFiles: string[];
  onlyLatest: boolean;
  onlyLatestActivities: boolean;
} {
  const dailyFiles: string[] = [];
  const activityFiles: string[] = [];
  let onlyLatest = false;
  let onlyLatestActivities = false;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--file' && argv[i + 1]) {
      dailyFiles.push(argv[i + 1]);
      i++;
      continue;
    }
    if (arg === '--activities-file' && argv[i + 1]) {
      activityFiles.push(argv[i + 1]);
      i++;
      continue;
    }
    if (arg === '--only-latest') {
      onlyLatest = true;
    }
    if (arg === '--activities-only-latest') {
      onlyLatestActivities = true;
    }
  }

  return { dailyFiles, activityFiles, onlyLatest, onlyLatestActivities };
}

export class GarminDataImporter {
  private pgClient: pg.Client;
  private stats: { [key: string]: number } = {};
  private userId: string = process.env.GARMIN_USER_ID || 'garmin_user';
  private exportsDir: string;

  constructor() {
    this.pgClient = new pg.Client(resolvePostgresConfig());
    this.exportsDir = resolveExportsDir();
  }

  async connect(): Promise<void> {
    await this.pgClient.connect();
    console.log('✅ Connected to PostgreSQL');
  }

  async disconnect(): Promise<void> {
    await this.pgClient.end();
  }

  async createBackupRecord(startDate: string, endDate: string): Promise<number> {
    const result = await this.pgClient.query(`
      INSERT INTO backup_metadata (backup_type, start_date, end_date, status)
      VALUES ('garmin_import', $1, $2, 'started')
      RETURNING id
    `, [startDate, endDate]);
    
    return result.rows[0].id;
  }

  async updateBackupRecord(id: number, recordsCount: number, status: string): Promise<void> {
    await this.pgClient.query(`
      UPDATE backup_metadata 
      SET records_count = $1, status = $2, completed_at = CURRENT_TIMESTAMP
      WHERE id = $3
    `, [recordsCount, status, id]);
  }

  async importUserProfile(userData: any): Promise<void> {
    if (!userData) return;
    
    console.log('👤 Importing user profile...');
    
    try {
      await this.pgClient.query(`
        INSERT INTO user_profile (user_id, display_name, email)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_id) DO UPDATE SET
          display_name = EXCLUDED.display_name,
          updated_at = CURRENT_TIMESTAMP
      `, [
        this.userId,
        userData.displayName || 'Garmin User',
        userData.email || null
      ]);

      this.stats['user_profile'] = 1;
      console.log('✅ User profile imported');
    } catch (error: any) {
      console.error('❌ Error importing user profile:', error.message);
    }
  }

  async importDailyHealthMetrics(data: GarminExportData): Promise<void> {
    console.log('\n📊 Importing daily health metrics...');
    
    let imported = 0;
    let errors = 0;

    for (const [date, dayData] of Object.entries(data.data)) {
      try {
        if (!dayData.stats) continue;
        
        const stats = dayData.stats;
        const heartRate = dayData.heart_rate;
        const sleep = dayData.sleep;
        
        await this.pgClient.query(`
          INSERT INTO daily_health_metrics (
            date, user_id, total_steps, total_distance_meters, 
            active_calories, resting_calories, total_calories,
            resting_heart_rate, min_heart_rate, max_heart_rate,
            hydration_goal_ml, hydration_intake_ml
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
          ON CONFLICT (date, user_id) DO UPDATE SET
            total_steps = EXCLUDED.total_steps,
            total_distance_meters = EXCLUDED.total_distance_meters,
            active_calories = EXCLUDED.active_calories,
            resting_calories = EXCLUDED.resting_calories,
            total_calories = EXCLUDED.total_calories,
            resting_heart_rate = EXCLUDED.resting_heart_rate,
            min_heart_rate = EXCLUDED.min_heart_rate,
            max_heart_rate = EXCLUDED.max_heart_rate,
            hydration_goal_ml = EXCLUDED.hydration_goal_ml,
            hydration_intake_ml = EXCLUDED.hydration_intake_ml
        `, [
          date,
          this.userId,
          stats.totalSteps || null,
          stats.totalDistanceMeters || null,
          stats.activeKilocalories || null,
          stats.bmrKilocalories || null,
          stats.totalKilocalories || null,
          heartRate?.restingHeartRate || null,
          heartRate?.minHeartRate || null,
          heartRate?.maxHeartRate || null,
          dayData.hydration?.hydrationGoal || null,
          dayData.hydration?.hydrationIntake || null
        ]);

        imported++;
        if (imported % 50 === 0) {
          console.log(`  📊 Processed ${imported} daily records...`);
        }
        
      } catch (error: any) {
        console.error(`❌ Error importing daily data for ${date}:`, error.message);
        errors++;
      }
    }

    this.stats['daily_health_metrics'] = imported;
    console.log(`✅ Imported ${imported} daily health records (${errors} errors)`);
  }

  async importSleepData(data: GarminExportData): Promise<void> {
    console.log('\n😴 Importing sleep data...');
    
    let imported = 0;
    let errors = 0;

    for (const [date, dayData] of Object.entries(data.data)) {
      try {
        if (!dayData.sleep) continue;
        
        const sleep = dayData.sleep;
        
        await this.pgClient.query(`
          INSERT INTO sleep_sessions (
            user_id, sleep_date, sleep_duration_seconds,
            deep_sleep_seconds, light_sleep_seconds, rem_sleep_seconds, awake_seconds,
            sleep_start_time, sleep_end_time, sleep_score
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          ON CONFLICT (user_id, sleep_date) DO UPDATE SET
            sleep_duration_seconds = EXCLUDED.sleep_duration_seconds,
            deep_sleep_seconds = EXCLUDED.deep_sleep_seconds,
            light_sleep_seconds = EXCLUDED.light_sleep_seconds,
            rem_sleep_seconds = EXCLUDED.rem_sleep_seconds,
            awake_seconds = EXCLUDED.awake_seconds,
            sleep_start_time = EXCLUDED.sleep_start_time,
            sleep_end_time = EXCLUDED.sleep_end_time,
            sleep_score = EXCLUDED.sleep_score
        `, [
          this.userId,
          date,
          sleep.totalSleepTimeSeconds || null,
          sleep.deepSleepSeconds || null,
          sleep.lightSleepSeconds || null,
          sleep.remSleepSeconds || null,
          sleep.awakeSleepSeconds || null,
          sleep.sleepStartTimestampLocal ? new Date(sleep.sleepStartTimestampLocal) : null,
          sleep.sleepEndTimestampLocal ? new Date(sleep.sleepEndTimestampLocal) : null,
          sleep.overallSleepScore || null
        ]);

        imported++;
        
      } catch (error: any) {
        console.error(`❌ Error importing sleep data for ${date}:`, error.message);
        errors++;
      }
    }

    this.stats['sleep_sessions'] = imported;
    console.log(`✅ Imported ${imported} sleep sessions (${errors} errors)`);
  }

  async importActivities(files?: string[]): Promise<void> {
    console.log('\n🏃 Importing activities...');
    
    // Find activities file
    const activityFiles = files?.length
      ? files
      : fs.readdirSync(this.exportsDir)
          .filter(file => file.includes('activities') && file.endsWith('.json'));

    if (activityFiles.length === 0) {
      console.log('⚠️ No activities files found');
      return;
    }

    let totalImported = 0;
    let totalErrors = 0;

    for (const file of activityFiles) {
      console.log(`📂 Processing ${file}...`);
      
      const filePath = path.join(this.exportsDir, file);
      const activitiesData: ActivityData = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
      
      console.log(`📊 Found ${activitiesData.metadata.activity_count} activities in file`);
      
      let imported = 0;
      let errors = 0;

      for (const activityWrapper of activitiesData.activities) {
        const activity = activityWrapper.summary;
        
        try {
          await this.pgClient.query(`
            INSERT INTO activities (
              activity_id, user_id, activity_name, activity_type,
              start_time_local, start_time_gmt, duration_seconds, distance_meters,
              calories, bmr_calories, average_hr, max_hr, average_speed, max_speed,
              elevation_gain, elevation_loss, begin_latitude, begin_longitude,
              has_polyline, active_calories
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
            ON CONFLICT (activity_id) DO UPDATE SET
              activity_name = EXCLUDED.activity_name,
              duration_seconds = EXCLUDED.duration_seconds,
              distance_meters = EXCLUDED.distance_meters,
              calories = EXCLUDED.calories,
              bmr_calories = EXCLUDED.bmr_calories,
              average_hr = EXCLUDED.average_hr,
              max_hr = EXCLUDED.max_hr,
              average_speed = EXCLUDED.average_speed,
              max_speed = EXCLUDED.max_speed,
              elevation_gain = EXCLUDED.elevation_gain,
              elevation_loss = EXCLUDED.elevation_loss
          `, [
            activity.activityId,
            this.userId,
            activity.activityName || null,
            activity.activityType?.typeKey || null,
            activity.startTimeLocal ? new Date(activity.startTimeLocal) : null,
            activity.startTimeGMT ? new Date(activity.startTimeGMT) : null,
            Math.round(activity.duration || 0),
            activity.distance || null,
            activity.calories || null,
            activity.bmrCalories || null,
            activity.averageHR || null,
            activity.maxHR || null,
            activity.averageSpeed || null,
            activity.maxSpeed || null,
            activity.elevationGain || null,
            activity.elevationLoss || null,
            activity.startLatitude || null,
            activity.startLongitude || null,
            activity.hasPolyline || false,
            activity.activeKilocalories || null
          ]);

          imported++;
          console.log(`✅ Imported activity: ${activity.activityName} (${activity.activityId})`);
          
        } catch (error: any) {
          console.error(`❌ Error importing activity ${activity.activityId}:`, error.message);
          errors++;
        }
      }
      
      totalImported += imported;
      totalErrors += errors;
      console.log(`✅ Processed ${imported} activities from ${file} (${errors} errors)`);
    }

    this.stats['activities'] = totalImported;
    console.log(`✅ Total imported ${totalImported} activities (${totalErrors} errors)`);
  }

  async importBodyComposition(data: GarminExportData): Promise<void> {
    console.log('\n⚖️ Importing body composition data...');
    
    let imported = 0;
    let errors = 0;

    for (const [date, dayData] of Object.entries(data.data)) {
      try {
        if (!dayData.body_composition) continue;
        
        const bodyComp = dayData.body_composition;
        
        await this.pgClient.query(`
          INSERT INTO body_composition (
            measurement_date, user_id, weight_kg, body_fat_percentage,
            muscle_mass_kg, bone_mass_kg, bmi
          ) VALUES ($1, $2, $3, $4, $5, $6, $7)
          ON CONFLICT (measurement_date, user_id) DO UPDATE SET
            weight_kg = EXCLUDED.weight_kg,
            body_fat_percentage = EXCLUDED.body_fat_percentage,
            muscle_mass_kg = EXCLUDED.muscle_mass_kg,
            bone_mass_kg = EXCLUDED.bone_mass_kg,
            bmi = EXCLUDED.bmi
        `, [
          date,
          this.userId,
          bodyComp.weight || null,
          bodyComp.bodyFat || null,
          bodyComp.muscleMass || null,
          bodyComp.boneMass || null,
          bodyComp.bmi || null
        ]);

        imported++;
        
      } catch (error: any) {
        console.error(`❌ Error importing body composition for ${date}:`, error.message);
        errors++;
      }
    }

    this.stats['body_composition'] = imported;
    console.log(`✅ Imported ${imported} body composition records (${errors} errors)`);
  }

  async processExportFile(filename: string): Promise<void> {
    console.log(`\n📂 Processing ${filename}...`);
    
    const filePath = path.join(this.exportsDir, filename);
    if (!fs.existsSync(filePath)) {
      console.log(`⚠️ File not found: ${filename}`);
      return;
    }

    const data: GarminExportData = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
    
    console.log(`📅 Date range: ${data.metadata.start_date} to ${data.metadata.end_date}`);
    console.log(`📊 Data types: ${data.metadata.data_types.join(', ')}`);
    
    // Extract user profile from first day's data (if available)
    const firstDay = Object.values(data.data)[0];
    if (firstDay?.user_summary) {
      await this.importUserProfile(firstDay.user_summary);
    }

    // Import data by type
    await this.importDailyHealthMetrics(data);
    await this.importSleepData(data);
    await this.importBodyComposition(data);
  }

  async verifyImport(): Promise<void> {
    console.log('\n🔍 Verifying import...');

    const checks: Array<{ table: string; dateExpr?: string }> = [
      { table: 'user_profile' },
      { table: 'daily_health_metrics', dateExpr: 'date::text' },
      { table: 'sleep_sessions', dateExpr: 'sleep_date::text' },
      { table: 'activities', dateExpr: 'start_time_local::date::text' },
      { table: 'body_composition', dateExpr: 'measurement_date::text' },
    ];

    for (const check of checks) {
      try {
        if (check.dateExpr) {
          const result = await this.pgClient.query(
            `SELECT COUNT(*) as count, MIN(${check.dateExpr}) as first_date, MAX(${check.dateExpr}) as last_date FROM ${check.table}`
          );
          console.log(
            `📊 ${check.table}: ${result.rows[0].count} rows (${result.rows[0].first_date} to ${result.rows[0].last_date})`
          );
        } else {
          const result = await this.pgClient.query(`SELECT COUNT(*) as count FROM ${check.table}`);
          console.log(`📊 ${check.table}: ${result.rows[0].count} rows`);
        }
      } catch (error) {
        console.log(`📊 ${check.table}: Error getting count - ${error}`);
      }
    }
  }

  async run(): Promise<void> {
    try {
      await this.connect();
      
      const args = parseArgs(process.argv.slice(2));

      // Get all daily export files
      let exportFiles = args.dailyFiles.length
        ? args.dailyFiles
        : fs.readdirSync(this.exportsDir)
            .filter(file => file.includes('garmin_daily_') && file.endsWith('.json'))
            .sort();

      if (args.onlyLatest && exportFiles.length > 1) {
        exportFiles = [exportFiles[exportFiles.length - 1]];
      }

      console.log(`🚀 Found ${exportFiles.length} export files to process`);
      console.log('📁 Files:', exportFiles);

      if (exportFiles.length === 0) {
        console.log('❌ No export files found! Please run data extraction first.');
        return;
      }

      // Process each export file
      for (const file of exportFiles) {
        await this.processExportFile(file);
      }

      // Import activities
      if (args.activityFiles.length) {
        await this.importActivities(args.activityFiles);
      } else if (args.onlyLatestActivities) {
        const all = fs
          .readdirSync(this.exportsDir)
          .filter(file => file.includes('activities') && file.endsWith('.json'))
          .sort();
        const latest = all.length ? [all[all.length - 1]] : [];
        await this.importActivities(latest);
      } else {
        await this.importActivities();
      }
      
      // Verify the import
      await this.verifyImport();
      
      const totalRecords = Object.values(this.stats).reduce((sum, count) => sum + count, 0);
      
      console.log('\n✅ Import completed successfully!');
      console.log('📊 Summary:', this.stats);
      console.log(`📈 Total records: ${totalRecords}`);
      
    } catch (error: any) {
      console.error('❌ Import failed:', error);
      throw error;
    } finally {
      await this.disconnect();
    }
  }
}

// Run the import
if (process.argv[1] && process.argv[1].endsWith('import_extracted_data.js')) {
  const span = tracer.startSpan('garmin.etl.import', {
    attributes: {
      'garmin.exports_dir': resolveExportsDir(),
      'db.name': process.env.DATABASE_NAME || 'garmin_backup',
    }
  });

  const importer = new GarminDataImporter();
  importer
    .run()
    .then(() => span.setStatus({ code: SpanStatusCode.OK }))
    .catch((err) => {
      span.recordException(err as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: String(err) });
      throw err;
    })
    .finally(async () => {
      span.end();
      await shutdownTracing();
    });
}
