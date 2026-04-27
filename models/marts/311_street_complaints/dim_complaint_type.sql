WITH complaint_types AS (
    SELECT DISTINCT
        complaint_type,
        descriptor        AS complaint_description,
        descriptor_2
    FROM {{ ref('stg_311_street_complaint') }}
    WHERE complaint_type IS NOT NULL
),
dim_complaint_type AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY complaint_type) AS complaint_sgkey,
        complaint_type,
        complaint_description,
         CASE
            -- ----------------------------------------------------------------
            -- 1. SIGNAL / LIGHTING FAILURE
            --    Descriptors indicating that a traffic signal or street light
            --    is not working correctly: fully out, dim, wrong color,
            --    out of sequence, or physically rotated off-axis.
            -- ----------------------------------------------------------------
            WHEN descriptor_2 IN (
                'Allout',
                'Emergency (All Out)',
                'Out',
                'Down',
                'Dim (Check Neutral)',
                'Improper Color',
                'Out Of Sequence',
                'Out Of Time',
                'Operating Improperly',
                'Steady',
                'Turned'
            ) THEN 'Signal / Lighting Failure'
 
            -- ----------------------------------------------------------------
            -- 2. PHYSICAL / STRUCTURAL DAMAGE
            --    Conditions describing material deterioration of a component:
            --    broken, cracked, hanging, loose, exposed, or open.
            --    These are the most frequent descriptors in the dataset (>600 cases).
            -- ----------------------------------------------------------------
            WHEN descriptor_2 IN (
                'Damaged',
                'Cracked',
                'Exposed',
                'Hanging',
                'Leaning',
                'Loose',
                'Open'
            ) THEN 'Physical / Structural Damage'
 
            -- ----------------------------------------------------------------
            -- 3. MISSING COMPONENT
            --    Separated from "Physical Damage" because it implies replacement,
            --    not repair — requiring a distinct inventory action.
            -- ----------------------------------------------------------------
            WHEN descriptor_2 = 'Missing'
            THEN 'Missing Component'
 
            -- ----------------------------------------------------------------
            -- 4. VANDALISM / GRAFFITI
            --    Intentional damage. Kept separate from natural deterioration
            --    to enable analysis of criminal incidence by area.
            -- ----------------------------------------------------------------
            WHEN descriptor_2 IN (
                'Vandalized',
                'Graffiti'
            ) THEN 'Vandalism / Graffiti'
 
            -- ----------------------------------------------------------------
            -- 5. STREET LIGHT LOCATION TYPE
            --    For street light complaints, descriptor_2 records the type
            --    of road where the light is located (highway, intersection, park...).
            --    Grouping them enables geospatial analysis by infrastructure type.
            -- ----------------------------------------------------------------
            WHEN descriptor_2 IN (
                'Location Type: Bridge / Tunnel',
                'Location Type: Highway',
                'Location Type: Intersection',
                'Location Type: Misc.',
                'Location Type: On-Street with Cross',
                'Location Type: Other',
                'Location Type: Park',
                'Location Type: Residential Address',
                'Overpass/Traffic Median'
            ) THEN 'Street Light Location Type'
            
 
            -- ----------------------------------------------------------------
            -- 6. SIDEWALK / CURB INFRASTRUCTURE
            --    Indicates the type or owner of the affected sidewalk/curb.
            --    Relevant for attributing repair responsibility
            --    (property owner vs. city).
            -- ----------------------------------------------------------------
            WHEN descriptor_2 IN (
                '1-3 Residential Units',
                '4 + Residential Units/Commercial',
                'City-Owned',
                'Curb Cut'
            ) THEN 'Sidewalk / Curb Infrastructure'
 
            -- ----------------------------------------------------------------
            -- 7. SIDEWALK VIOLATION PROCESS
            --    Administrative states in the lifecycle of a sidewalk violation:
            --    inquiry, inspection, closure by owner repair or city repair.
            -- ----------------------------------------------------------------
            WHEN descriptor_2 IN (
                'Curb Violation Inquiry',
                'Sidewalk Violation Inquiry',
                'Sidewalk Re-inspection Request',
                'Dismiss Violation City Fixed',
                'Dismiss Violation Owner Fixed',
                'Request City Repair'
            ) THEN 'Sidewalk Violation Process'
 
            -- ----------------------------------------------------------------
            -- 8. OPERATIONAL SERVICE MANAGEMENT
            --    Active maintenance actions: installation, removal, transfer,
            --    cleaning, and utility verification.
            --    These are not spontaneous failures but planned work orders.
            -- ----------------------------------------------------------------
            WHEN descriptor_2 IN (
                'Install',
                'Remove',
                'Transfer',
                'Obscured (Dirty)',
                'Need Door Opened',
                'Need Utility Verify',
                'Advertisement'
            ) THEN 'Operational Service Management'
 
            -- ----------------------------------------------------------------
            -- 9. DRAINAGE / SEWER
            --    Internal DEP (Dept. of Environmental Protection) codes for
            --    catch basins and drains. SA* = drain type,
            --    SB*/SC*/SE/SH/SJ = drainage system condition,
            --    SRGD*/SRGFLD = flooding/damage reports, GIRG* = GIR codes.
            -- ----------------------------------------------------------------
            WHEN descriptor_2 IN (
                'SA', 'SA1', 'SA2', 'SA3', 'SA4',
                'SB', 'SB4', 'SB5',
                'SC', 'SC1', 'SC2', 'SC4',
                'SE', 'SG1', 'SH', 'SJ',
                'SRGDBR', 'SRGDM', 'SRGFLD', 'SZZ',
                'GIRGD', 'GIRGP', 'GIRGS'
            ) THEN 'Drainage / Sewer'
 
            -- ----------------------------------------------------------------
            -- 10. UNCLASSIFIED
            --     Generic values (Other, N/A) or nulls that do not fit
            --     any defined operational category.
            -- ----------------------------------------------------------------
            ELSE 'Unclassified'
 
        END AS complaint_category
    FROM complaint_types
)

SELECT * FROM dim_complaint_type