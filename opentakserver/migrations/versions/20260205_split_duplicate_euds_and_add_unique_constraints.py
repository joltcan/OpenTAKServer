"""Split duplicate EUD assignments and add unique constraints

This migration handles duplicate eud_uid values in certificates and data_packages by:
1. Cloning EUD rows for each duplicate certificate/data_package
2. Reassigning duplicates to the cloned EUD rows
3. Adding unique constraints to enforce 1-to-1 relationships

Revision ID: 20260205_split_duplicate_euds
Revises: 591a98184047
Create Date: 2026-02-05 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "20260205_split_duplicate_euds"
down_revision = "591a98184047"
branch_labels = None
depends_on = None


def upgrade():
    """
    Split duplicate EUDs by cloning originals with ::dup::<id> suffix.
    This preserves all data and allows adding unique constraints.
    """
    # Get a database connection to execute raw SQL
    connection = op.get_bind()
    
    # Step 1: Handle duplicate certificates.eud_uid
    # Find all eud_uid values with multiple certificates
    dup_certs_query = """
    SELECT eud_uid, array_agg(id ORDER BY id) as cert_ids
    FROM certificates
    WHERE eud_uid IS NOT NULL
    GROUP BY eud_uid
    HAVING count(*) > 1
    """
    
    # Step 2: For each duplicate eud_uid in certificates, clone the EUD and reassign
    clone_euds_for_certs = """
    WITH dups AS (
        SELECT eud_uid, array_agg(id ORDER BY id) as cert_ids
        FROM certificates
        WHERE eud_uid IS NOT NULL
        GROUP BY eud_uid
        HAVING count(*) > 1
    ),
    to_clone AS (
        SELECT DISTINCT e.* 
        FROM euds e
        JOIN dups d ON e.uid = d.eud_uid
    ),
    cloned AS (
        INSERT INTO euds (
            uid, callsign, device, os, platform, version, phone_number,
            last_event_time, last_status, user_id, team_id, team_role,
            meshtastic_id, meshtastic_macaddr, last_meshtastic_publish
        )
        SELECT 
            e.uid || '::dup::' || c.id,
            e.callsign,
            e.device,
            e.os,
            e.platform,
            e.version,
            e.phone_number,
            e.last_event_time,
            e.last_status,
            e.user_id,
            e.team_id,
            e.team_role,
            e.meshtastic_id,
            e.meshtastic_macaddr,
            e.last_meshtastic_publish
        FROM to_clone e
        CROSS JOIN certificates c
        WHERE c.eud_uid = e.uid
          AND c.id != (
            SELECT (array_agg(id ORDER BY id))[1]
            FROM certificates c2
            WHERE c2.eud_uid = e.uid
          )
        RETURNING uid, SUBSTRING(uid FROM 1 FOR POSITION('::dup::' IN uid) - 1) as orig_uid
    )
    UPDATE certificates c
    SET eud_uid = cloned.uid
    FROM cloned
    WHERE c.eud_uid = cloned.orig_uid
      AND c.id::text LIKE '%' || SUBSTRING(cloned.uid FROM POSITION('::dup::' IN cloned.uid) + 7)
    """
    
    # Simpler approach: use Python loop to handle each duplicate carefully
    # First get all duplicates
    dup_certs = connection.execute(
        sa.text("""
        SELECT eud_uid, array_agg(id ORDER BY id) as cert_ids
        FROM certificates
        WHERE eud_uid IS NOT NULL
        GROUP BY eud_uid
        HAVING count(*) > 1
        """)
    ).fetchall()
    
    # For each duplicate eud_uid, keep the first cert and clone EUD for the rest
    for row in dup_certs:
        eud_uid = row[0]
        cert_ids = row[1]
        keep_cert_id = cert_ids[0]
        dup_cert_ids = cert_ids[1:]
        
        # Get the original EUD data
        eud_row = connection.execute(
            sa.text("""
            SELECT id, callsign, device, os, platform, version, phone_number,
                   last_event_time, last_status, user_id, team_id, team_role,
                   meshtastic_id, meshtastic_macaddr, last_meshtastic_publish
            FROM euds
            WHERE uid = :uid
            """),
            {"uid": eud_uid}
        ).fetchone()
        
        if eud_row:
            # Clone EUD for each duplicate certificate
            for cert_id in dup_cert_ids:
                new_uid = f"{eud_uid}::dup::{cert_id}"
                
                # Insert cloned EUD
                connection.execute(
                    sa.text("""
                    INSERT INTO euds (
                        uid, callsign, device, os, platform, version, phone_number,
                        last_event_time, last_status, user_id, team_id, team_role,
                        meshtastic_id, meshtastic_macaddr, last_meshtastic_publish
                    ) VALUES (
                        :uid, :callsign, :device, :os, :platform, :version, :phone_number,
                        :last_event_time, :last_status, :user_id, :team_id, :team_role,
                        :meshtastic_id, :meshtastic_macaddr, :last_meshtastic_publish
                    )
                    """),
                    {
                        "uid": new_uid,
                        "callsign": eud_row[1],
                        "device": eud_row[2],
                        "os": eud_row[3],
                        "platform": eud_row[4],
                        "version": eud_row[5],
                        "phone_number": eud_row[6],
                        "last_event_time": eud_row[7],
                        "last_status": eud_row[8],
                        "user_id": eud_row[9],
                        "team_id": eud_row[10],
                        "team_role": eud_row[11],
                        "meshtastic_id": eud_row[12],
                        "meshtastic_macaddr": eud_row[13],
                        "last_meshtastic_publish": eud_row[14],
                    }
                )
                
                # Update duplicate certificate to point to cloned EUD
                connection.execute(
                    sa.text("""
                    UPDATE certificates
                    SET eud_uid = :new_uid
                    WHERE id = :cert_id
                    """),
                    {"new_uid": new_uid, "cert_id": cert_id}
                )
    
    # Step 3: Handle duplicate data_packages.creator_uid
    dup_pkgs = connection.execute(
        sa.text("""
        SELECT creator_uid, array_agg(id ORDER BY id) as pkg_ids
        FROM data_packages
        WHERE creator_uid IS NOT NULL
        GROUP BY creator_uid
        HAVING count(*) > 1
        """)
    ).fetchall()
    
    # For each duplicate creator_uid, keep the first package and clone EUD for the rest
    for row in dup_pkgs:
        creator_uid = row[0]
        pkg_ids = row[1]
        keep_pkg_id = pkg_ids[0]
        dup_pkg_ids = pkg_ids[1:]
        
        # Get the original EUD data
        eud_row = connection.execute(
            sa.text("""
            SELECT id, callsign, device, os, platform, version, phone_number,
                   last_event_time, last_status, user_id, team_id, team_role,
                   meshtastic_id, meshtastic_macaddr, last_meshtastic_publish
            FROM euds
            WHERE uid = :uid
            """),
            {"uid": creator_uid}
        ).fetchone()
        
        if eud_row:
            # Clone EUD for each duplicate data_package
            for pkg_id in dup_pkg_ids:
                new_uid = f"{creator_uid}::dup::{pkg_id}"
                
                # Check if this cloned EUD already exists (from certificate dedupe)
                existing = connection.execute(
                    sa.text("SELECT 1 FROM euds WHERE uid = :uid"),
                    {"uid": new_uid}
                ).fetchone()
                
                if not existing:
                    # Insert cloned EUD
                    connection.execute(
                        sa.text("""
                        INSERT INTO euds (
                            uid, callsign, device, os, platform, version, phone_number,
                            last_event_time, last_status, user_id, team_id, team_role,
                            meshtastic_id, meshtastic_macaddr, last_meshtastic_publish
                        ) VALUES (
                            :uid, :callsign, :device, :os, :platform, :version, :phone_number,
                            :last_event_time, :last_status, :user_id, :team_id, :team_role,
                            :meshtastic_id, :meshtastic_macaddr, :last_meshtastic_publish
                        )
                        """),
                        {
                            "uid": new_uid,
                            "callsign": eud_row[1],
                            "device": eud_row[2],
                            "os": eud_row[3],
                            "platform": eud_row[4],
                            "version": eud_row[5],
                            "phone_number": eud_row[6],
                            "last_event_time": eud_row[7],
                            "last_status": eud_row[8],
                            "user_id": eud_row[9],
                            "team_id": eud_row[10],
                            "team_role": eud_row[11],
                            "meshtastic_id": eud_row[12],
                            "meshtastic_macaddr": eud_row[13],
                            "last_meshtastic_publish": eud_row[14],
                        }
                    )
                
                # Update duplicate data_package to point to cloned EUD
                connection.execute(
                    sa.text("""
                    UPDATE data_packages
                    SET creator_uid = :new_uid
                    WHERE id = :pkg_id
                    """),
                    {"new_uid": new_uid, "pkg_id": pkg_id}
                )
    
    # Step 4: Add unique constraints
    connection.execute(sa.text("""
    ALTER TABLE certificates
    ADD CONSTRAINT certificates_eud_uid_key UNIQUE (eud_uid)
    WHERE eud_uid IS NOT NULL
    """))
    
    connection.execute(sa.text("""
    ALTER TABLE data_packages
    ADD CONSTRAINT data_packages_creator_uid_key UNIQUE (creator_uid)
    WHERE creator_uid IS NOT NULL
    """))
    
    connection.commit()


def downgrade():
    """
    Downgrade by:
    1. Removing unique constraints
    2. Deleting cloned EUDs (those with ::dup:: in uid)
    3. Reassigning orphaned FKs back to original UIDs
    """
    connection = op.get_bind()
    
    # Remove unique constraints
    try:
        connection.execute(sa.text("""
        ALTER TABLE certificates
        DROP CONSTRAINT certificates_eud_uid_key
        """))
    except:
        pass
    
    try:
        connection.execute(sa.text("""
        ALTER TABLE data_packages
        DROP CONSTRAINT data_packages_creator_uid_key
        """))
    except:
        pass
    
    # Reassign certificates and data_packages back to original EUDs
    connection.execute(sa.text("""
    UPDATE certificates
    SET eud_uid = SUBSTRING(eud_uid FROM 1 FOR POSITION('::dup::' IN eud_uid) - 1)
    WHERE eud_uid LIKE '%::dup::%'
    """))
    
    connection.execute(sa.text("""
    UPDATE data_packages
    SET creator_uid = SUBSTRING(creator_uid FROM 1 FOR POSITION('::dup::' IN creator_uid) - 1)
    WHERE creator_uid LIKE '%::dup::%'
    """))
    
    # Delete cloned EUDs
    connection.execute(sa.text("""
    DELETE FROM euds
    WHERE uid LIKE '%::dup::%'
    """))
    
    connection.commit()
