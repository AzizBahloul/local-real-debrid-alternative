//! The instrument set: pixel-art wordmark, traffic graph, segmented meters,
//! command buttons, swarm rows and the colourised TTY.
//!
//! Every one of these is painted from rectangles and ASCII text. See the ASCII
//! rule in [`crate::theme`] -- the block and box-drawing characters that would
//! make this trivial are exactly the ones the bundled font does not promise.

use egui::{Color32, FontId, Rect, Response, Rounding, Sense, Stroke, Ui, Vec2};

use crate::fx;
use crate::theme::{self, AMBER, GRID, PHOSPHOR, PHOSPHOR_DIM, RED, TEXT, TEXT_DIM};

/// 5x5 cells per letter. Only the nine letters of NOVASTREAM exist -- a full
/// font would be dead weight for one wordmark.
fn letter(c: char) -> Option<[&'static str; 5]> {
    Some(match c {
        'N' => ["#   #", "##  #", "# # #", "#  ##", "#   #"],
        'O' => [" ### ", "#   #", "#   #", "#   #", " ### "],
        'V' => ["#   #", "#   #", "#   #", " # # ", "  #  "],
        'A' => [" ### ", "#   #", "#####", "#   #", "#   #"],
        'S' => [" ####", "#    ", " ### ", "    #", "#### "],
        'T' => ["#####", "  #  ", "  #  ", "  #  ", "  #  "],
        'R' => ["#### ", "#   #", "#### ", "#  # ", "#   #"],
        'E' => ["#####", "#    ", "#### ", "#    ", "#####"],
        'M' => ["#   #", "## ##", "# # #", "#   #", "#   #"],
        _ => return None,
    })
}

/// The wordmark, drawn as lit phosphor cells.
///
/// Pixel-art rather than a large font size for two reasons: it is the one
/// place a 1980s machine would have spent its character ROM, and it gives the
/// glitch below something to dislodge -- you cannot knock a column out of a
/// text string without reflowing it.
///
/// `boot` ramps 0->1 to sweep the letters in column by column on first paint;
/// `glitch` (0..1) tears columns sideways and splits the colour.
pub fn wordmark(ui: &mut Ui, text: &str, cell: f32, boot: f32, glitch: f32) -> Rect {
    let cols_per_letter = 5;
    let letters: Vec<[&'static str; 5]> = text.chars().filter_map(letter).collect();
    let total_cols = letters.len() * (cols_per_letter + 1);
    let size = Vec2::new(total_cols as f32 * cell, 5.0 * cell);
    let (rect, _) = ui.allocate_exact_size(size, Sense::hover());
    let painter = ui.painter();

    let mut rng = fx::Rng::new((glitch * 10_000.0) as u64 + 12_345);
    let lit = cell.max(2.0) - 0.6;

    for (letter_index, grid) in letters.iter().enumerate() {
        for (row, line) in grid.iter().enumerate() {
            for (col, ch) in line.chars().enumerate() {
                if ch != '#' {
                    continue;
                }
                let column = letter_index * (cols_per_letter + 1) + col;
                // Column-by-column reveal: a cell is dark until the boot
                // sweep has passed it.
                let reveal = boot * total_cols as f32;
                if (column as f32) > reveal {
                    continue;
                }

                let tear = if glitch > 0.0 && rng.next_f32() < glitch * 0.25 {
                    rng.range(-2.5, 2.5) * glitch
                } else {
                    0.0
                };
                let pos = egui::pos2(
                    rect.left() + column as f32 * cell + tear,
                    rect.top() + row as f32 * cell,
                );
                let cell_rect = Rect::from_min_size(pos, Vec2::splat(lit));

                if glitch > 0.0 {
                    // Convergence split: the two side-bands a misconverged
                    // tube throws either side of the beam. Both stay in the
                    // green family -- a red/blue flash here was the one
                    // rainbow moment in an otherwise mono-green window.
                    let bleed = 2.0 * glitch;
                    painter.rect_filled(
                        cell_rect.translate(Vec2::new(-bleed, 0.0)),
                        Rounding::ZERO,
                        Color32::from_rgba_unmultiplied(190, 255, 210, 70),
                    );
                    painter.rect_filled(
                        cell_rect.translate(Vec2::new(bleed, 0.0)),
                        Rounding::ZERO,
                        Color32::from_rgba_unmultiplied(26, 110, 56, 110),
                    );
                }
                // Bloom, then the cell itself.
                painter.rect_filled(
                    cell_rect.expand(1.2),
                    Rounding::ZERO,
                    Color32::from_rgba_unmultiplied(PHOSPHOR.r(), PHOSPHOR.g(), PHOSPHOR.b(), 34),
                );
                painter.rect_filled(cell_rect, Rounding::ZERO, PHOSPHOR);
            }
        }
    }
    rect
}

/// One traced line on the traffic graph.
pub struct Trace<'a> {
    pub samples: &'a [f32],
    pub color: Color32,
    pub label: &'a str,
}

/// Scrolling area chart for throughput.
///
/// Both traces share one auto-scaled axis: download and upload on independent
/// scales would make a 4 MiB/s download and a 40 KiB/s upload look identical,
/// which is the opposite of what a traffic graph is for. The scale floor stops
/// an idle gateway from rendering sensor noise as a mountain range.
pub fn traffic_graph(ui: &mut Ui, height: f32, traces: &[Trace<'_>], unit: &str) -> Rect {
    let (rect, _) = ui.allocate_exact_size(Vec2::new(ui.available_width(), height), Sense::hover());
    let painter = ui.painter();
    painter.rect_filled(rect, Rounding::ZERO, theme::BG_VOID);

    // Grid: four horizontal divisions and a dotted vertical every eighth.
    let grid_stroke = Stroke::new(1.0, GRID);
    for step in 1..4 {
        let y = rect.top() + rect.height() * step as f32 / 4.0;
        painter.line_segment(
            [egui::pos2(rect.left(), y), egui::pos2(rect.right(), y)],
            grid_stroke,
        );
    }
    for step in 1..8 {
        let x = rect.left() + rect.width() * step as f32 / 8.0;
        painter.line_segment(
            [egui::pos2(x, rect.top()), egui::pos2(x, rect.bottom())],
            Stroke::new(1.0, Color32::from_rgba_unmultiplied(20, 46, 32, 120)),
        );
    }

    let peak = traces
        .iter()
        .flat_map(|t| t.samples.iter().copied())
        .fold(0.0f32, f32::max)
        .max(0.5);

    for trace in traces {
        if trace.samples.len() < 2 {
            continue;
        }
        let step = rect.width() / (trace.samples.len() - 1).max(1) as f32;
        let point = |index: usize, value: f32| {
            egui::pos2(
                rect.left() + index as f32 * step,
                rect.bottom() - (value / peak).clamp(0.0, 1.0) * (rect.height() - 4.0),
            )
        };

        // Filled area, as a mesh so the fill fades out towards the baseline
        // instead of sitting there as a flat slab.
        let mut mesh = egui::Mesh::default();
        let top =
            Color32::from_rgba_unmultiplied(trace.color.r(), trace.color.g(), trace.color.b(), 70);
        let bottom =
            Color32::from_rgba_unmultiplied(trace.color.r(), trace.color.g(), trace.color.b(), 0);
        for (index, value) in trace.samples.iter().enumerate() {
            let p = point(index, *value);
            let base = mesh.vertices.len() as u32;
            mesh.colored_vertex(p, top);
            mesh.colored_vertex(egui::pos2(p.x, rect.bottom()), bottom);
            if index > 0 {
                mesh.add_triangle(base - 2, base - 1, base + 1);
                mesh.add_triangle(base - 2, base + 1, base);
            }
        }
        painter.add(egui::Shape::mesh(mesh));

        let line: Vec<egui::Pos2> = trace
            .samples
            .iter()
            .enumerate()
            .map(|(index, value)| point(index, *value))
            .collect();
        // Halo under the stroke: same trick as `theme::glow_text`.
        painter.add(egui::Shape::line(
            line.clone(),
            Stroke::new(
                3.0,
                Color32::from_rgba_unmultiplied(
                    trace.color.r(),
                    trace.color.g(),
                    trace.color.b(),
                    40,
                ),
            ),
        ));
        // Kept before the move so the head marker below still has it.
        let head = line.last().copied();
        painter.add(egui::Shape::line(line, Stroke::new(1.3, trace.color)));

        // A pulsing head on the newest sample, so a live graph is obviously
        // live even while the line is flat.
        if let Some(head) = head {
            painter.circle_filled(head, 2.4, trace.color);
            painter.circle_stroke(
                head,
                4.5,
                Stroke::new(
                    1.0,
                    Color32::from_rgba_unmultiplied(
                        trace.color.r(),
                        trace.color.g(),
                        trace.color.b(),
                        90,
                    ),
                ),
            );
        }
    }

    // Legend, and the axis peak so the shape has a magnitude attached.
    let mut x = rect.left() + 6.0;
    for trace in traces {
        painter.rect_filled(
            Rect::from_min_size(egui::pos2(x, rect.top() + 7.0), Vec2::new(7.0, 2.0)),
            Rounding::ZERO,
            trace.color,
        );
        let drawn = painter.text(
            egui::pos2(x + 11.0, rect.top() + 2.0),
            egui::Align2::LEFT_TOP,
            trace.label,
            FontId::monospace(10.0),
            trace.color,
        );
        x = drawn.right() + 12.0;
    }
    painter.text(
        egui::pos2(rect.right() - 6.0, rect.top() + 2.0),
        egui::Align2::RIGHT_TOP,
        format!("peak {peak:.2} {unit}"),
        FontId::monospace(10.0),
        TEXT_DIM,
    );

    rect
}

/// Segment count for every bar in the app. Discrete cells read as a readout;
/// a smooth fill reads as a web progress bar.
const SEGMENTS: usize = 28;

/// A labelled segmented bar: `LABEL [||||||......] value`.
///
/// The fill is animated through egui's own value animator keyed on `label`, so
/// a jumpy 1 Hz poll arrives as movement rather than as a series of snaps.
pub fn meter(ui: &mut Ui, label: &str, fraction: f32, value: &str, color: Color32) {
    let id = egui::Id::new(("meter", label));
    let target = fraction.clamp(0.0, 1.0);
    let shown = ui.ctx().animate_value_with_time(id, target, 0.4);

    let (rect, _) = ui.allocate_exact_size(Vec2::new(ui.available_width(), 14.0), Sense::hover());
    let painter = ui.painter();
    let font = FontId::monospace(11.0);

    painter.text(
        egui::pos2(rect.left(), rect.center().y),
        egui::Align2::LEFT_CENTER,
        label,
        font.clone(),
        TEXT_DIM,
    );
    let value_width = 78.0;
    let label_width = 56.0;
    let bar = Rect::from_min_max(
        egui::pos2(rect.left() + label_width, rect.top() + 3.0),
        egui::pos2(rect.right() - value_width, rect.bottom() - 3.0),
    );
    segmented_bar(painter, bar, shown, color);
    painter.text(
        egui::pos2(rect.right(), rect.center().y),
        egui::Align2::RIGHT_CENTER,
        value,
        font,
        TEXT,
    );
}

/// The bar itself, shared by the meters and the swarm rows.
pub fn segmented_bar(painter: &egui::Painter, rect: Rect, fraction: f32, color: Color32) {
    if rect.width() <= 1.0 {
        return;
    }
    let gap = 1.0;
    let seg_width = (rect.width() - gap * (SEGMENTS - 1) as f32) / SEGMENTS as f32;
    let lit = (fraction.clamp(0.0, 1.0) * SEGMENTS as f32).round() as usize;
    for index in 0..SEGMENTS {
        let x = rect.left() + index as f32 * (seg_width + gap);
        let cell = Rect::from_min_size(
            egui::pos2(x, rect.top()),
            Vec2::new(seg_width.max(1.0), rect.height()),
        );
        if index < lit {
            painter.rect_filled(
                cell.expand(0.8),
                Rounding::ZERO,
                Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), 30),
            );
            painter.rect_filled(cell, Rounding::ZERO, color);
        } else {
            painter.rect_filled(
                cell,
                Rounding::ZERO,
                Color32::from_rgba_unmultiplied(
                    PHOSPHOR_DIM.r(),
                    PHOSPHOR_DIM.g(),
                    PHOSPHOR_DIM.b(),
                    60,
                ),
            );
        }
    }
}

/// A full-width command button drawn as a terminal prompt.
///
/// Deliberately *not* an `egui::Button`: the hover state is a filled sweep and
/// a bracket highlight rather than a colour swap, and a disabled button still
/// shows its label at reduced contrast so the operator can read what will
/// happen once it becomes available.
pub fn command_button(
    ui: &mut Ui,
    label: &str,
    color: Color32,
    enabled: bool,
    height: f32,
) -> Response {
    let (rect, response) = ui.allocate_exact_size(
        Vec2::new(ui.available_width(), height),
        if enabled {
            Sense::click()
        } else {
            Sense::hover()
        },
    );
    let hover = ui
        .ctx()
        .animate_bool_with_time(response.id, enabled && response.hovered(), 0.12);
    let time = ui.input(|i| i.time);

    let painter = ui.painter();
    let base = if enabled { color } else { TEXT_DIM };
    let fill_alpha = (14.0 + hover * 46.0) as u8;
    painter.rect_filled(
        rect,
        Rounding::ZERO,
        Color32::from_rgba_unmultiplied(base.r(), base.g(), base.b(), fill_alpha),
    );
    painter.rect_stroke(
        rect,
        Rounding::ZERO,
        Stroke::new(1.0, if enabled { base } else { GRID }),
    );
    theme::corner_brackets(
        painter,
        rect.shrink(2.0),
        6.0 + hover * 5.0,
        Stroke::new(1.0, base),
    );

    let font = FontId::monospace((height * 0.28).clamp(11.0, 17.0));
    let caret = if enabled && fx::cursor_on(time) {
        "> "
    } else {
        "  "
    };
    theme::glow_text(
        painter,
        rect.center(),
        egui::Align2::CENTER_CENTER,
        &format!("{caret}{label}"),
        font,
        if enabled {
            base
        } else {
            Color32::from_rgba_unmultiplied(base.r(), base.g(), base.b(), 130)
        },
    );

    if enabled {
        response.on_hover_cursor(egui::CursorIcon::PointingHand)
    } else {
        response
    }
}

/// A small ghost button (`[copy]`, `[cancel]`) that keeps the bracket idiom.
pub fn ghost_button(ui: &mut Ui, label: &str, color: Color32) -> Response {
    let font = FontId::monospace(10.0);
    let text = format!("[{label}]");
    let galley = ui.painter().layout_no_wrap(text, font, color);
    let (rect, response) =
        ui.allocate_exact_size(galley.size() + Vec2::new(8.0, 4.0), Sense::click());
    let hovered = response.hovered();
    if hovered {
        ui.painter().rect_filled(
            rect,
            Rounding::ZERO,
            Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), 34),
        );
    }
    ui.painter()
        .galley(rect.center() - galley.size() / 2.0, galley, color);
    response.on_hover_cursor(egui::CursorIcon::PointingHand)
}

/// The blinking-LED status readout at the top right of the HUD.
pub fn status_lamp(ui: &mut Ui, label: &str, color: Color32, pulse: bool) {
    let time = ui.input(|i| i.time);
    let (rect, _) = ui.allocate_exact_size(Vec2::new(ui.available_width(), 16.0), Sense::hover());
    let painter = ui.painter();

    let centre = egui::pos2(rect.left() + 6.0, rect.center().y);
    // A running lamp breathes; a stopped one just sits there. The difference
    // is visible from across the room, which is the point of a lamp.
    let breath = if pulse {
        0.55 + 0.45 * ((time * 2.6).sin() as f32 * 0.5 + 0.5)
    } else {
        1.0
    };
    painter.circle_filled(
        centre,
        5.5 * breath,
        Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), 46),
    );
    painter.circle_filled(centre, 3.0, color);
    theme::glow_text(
        painter,
        egui::pos2(rect.left() + 17.0, rect.center().y),
        egui::Align2::LEFT_CENTER,
        label,
        FontId::monospace(12.0),
        color,
    );
}

/// One torrent in the swarm panel: name, animated progress bar, and the live
/// numbers underneath.
/// What the buttons on one swarm row can ask for.
///
/// `Delete` is armed in two steps by the caller rather than here, because the
/// widget is redrawn from scratch every frame and has nowhere to keep the
/// "asked once already" bit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowAction {
    Pause,
    Resume,
    Delete,
}

/// One torrent: name, state, progress bar, stats, and its own controls.
///
/// `held` is the operator's own pause, which is not the same question as
/// `state == "paused"`: the download queue parks whatever is beyond its width,
/// and offering Resume for one of those would promise something the queue
/// immediately undoes. So a queue-parked row offers Pause (make it stay
/// parked) and a hand-paused one offers Resume.
pub fn swarm_row(
    ui: &mut Ui,
    name: &str,
    state: &str,
    held: bool,
    progress: f32,
    stats: &str,
    delete_armed: bool,
) -> Option<RowAction> {
    let font = FontId::monospace(11.0);
    let width = ui.available_width();
    let char_width = ui.fonts(|f| f.glyph_width(&font, 'M')).max(1.0);
    let mut action = None;

    ui.horizontal(|ui| {
        // Truncated rather than wrapped: a release name is 90 characters of
        // tags and wrapping it buries the bar. Cut by *characters*, never by
        // bytes -- a release title carrying one accented word would panic a
        // byte slice, and this string comes straight off the wire.
        //
        // The reserve grew with the buttons: the state word plus three
        // controls, or the name runs under them.
        let room = ((width - 210.0) / char_width).floor().max(8.0) as usize;
        let shown = if name.chars().count() > room {
            let head: String = name.chars().take(room.saturating_sub(1)).collect();
            format!("{head}~")
        } else {
            name.to_string()
        };
        ui.label(egui::RichText::new(shown).font(font.clone()).color(TEXT));
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            // Right-to-left, so these read delete, pause/resume, STATE from
            // the right edge inward.
            if delete_armed {
                if ghost_button(ui, "confirm delete", RED).clicked() {
                    action = Some(RowAction::Delete);
                }
            } else if ghost_button(ui, "delete", TEXT_DIM).clicked() {
                action = Some(RowAction::Delete);
            }
            if held {
                if ghost_button(ui, "resume", PHOSPHOR).clicked() {
                    action = Some(RowAction::Resume);
                }
            } else if ghost_button(ui, "pause", TEXT_DIM).clicked() {
                action = Some(RowAction::Pause);
            }
            ui.label(
                egui::RichText::new(if held {
                    "HELD".to_string()
                } else {
                    state.to_uppercase()
                })
                .font(FontId::monospace(10.0))
                .color(if held { AMBER } else { state_color(state) }),
            );
        });
    });

    let id = egui::Id::new(("swarm", name));
    let shown = ui
        .ctx()
        .animate_value_with_time(id, progress.clamp(0.0, 1.0), 0.5);
    let (rect, _) = ui.allocate_exact_size(Vec2::new(width, 9.0), Sense::hover());
    segmented_bar(ui.painter(), rect, shown, state_color(state));

    ui.label(
        egui::RichText::new(stats)
            .font(FontId::monospace(10.0))
            .color(TEXT_DIM),
    );
    if delete_armed {
        ui.label(
            egui::RichText::new("!! deletes this title's downloaded data")
                .font(FontId::monospace(10.0))
                .color(RED),
        );
    }
    action
}

fn state_color(state: &str) -> Color32 {
    match state.to_ascii_lowercase().as_str() {
        "live" | "seeding" => PHOSPHOR,
        "initializing" | "paused" => AMBER,
        "error" => RED,
        _ => TEXT,
    }
}

/// Colour for one TTY line.
///
/// Keyed on what the line *is*, not on where it came from: the gateway logs
/// through `tracing` on stdout and a panic arrives on stderr, so severity has
/// to be read out of the text either way.
pub fn log_color(line: &str) -> Color32 {
    let lower = line.to_ascii_lowercase();
    if lower.contains("[stderr]")
        || lower.contains("error")
        || lower.contains("panic")
        || lower.contains("failed")
    {
        RED
    } else if lower.contains("warn") {
        AMBER
    } else if lower.contains("http://") || lower.contains("https://") {
        // URLs step up from the dim chatter, but stay in the green family.
        TEXT
    } else if lower.contains(" ok") || lower.contains("ready") || lower.contains("listening") {
        PHOSPHOR
    } else {
        TEXT_DIM
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wordmark can only draw letters it has cells for; a silently
    /// dropped letter would spell something else.
    #[test]
    fn every_letter_of_the_wordmark_exists() {
        for c in "NOVASTREAM".chars() {
            assert!(letter(c).is_some(), "no cells for {c}");
        }
    }

    /// Every row of every letter must be exactly five cells wide, else the
    /// glyph shears sideways as it is drawn.
    #[test]
    fn letter_cells_are_five_by_five() {
        for c in "NOVASTREAM".chars() {
            let grid = letter(c).unwrap();
            assert_eq!(grid.len(), 5, "{c} is not five rows");
            for row in grid {
                assert_eq!(row.chars().count(), 5, "{c} has a row of width != 5");
            }
        }
    }

    #[test]
    fn log_severity_wins_over_the_url_highlight() {
        assert_eq!(log_color("[stderr] failed to bind http://x"), RED);
        assert_eq!(log_color("WARN slow peer"), AMBER);
        assert_eq!(log_color("   http://192.168.1.67:8080"), TEXT);
        assert_eq!(log_color("plain chatter"), TEXT_DIM);
    }

    #[test]
    fn torrent_states_map_onto_the_palette() {
        assert_eq!(state_color("live"), PHOSPHOR);
        assert_eq!(state_color("initializing"), AMBER);
        assert_eq!(state_color("error"), RED);
    }
}
