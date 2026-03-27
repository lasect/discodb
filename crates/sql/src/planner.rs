use crate::error::SqlResult;
use crate::parser::Statement;
use discodb_executor::{PhysicalPlan, operators::SeqScan};

pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self
    }

    pub fn plan(&self, stmt: Statement) -> SqlResult<PhysicalPlan> {
        match stmt {
            Statement::Select(_select) => self.plan_simple_select(),
            _ => Err(crate::error::SqlError::Unsupported(
                "only SELECT is supported".to_string(),
            )),
        }
    }

    fn plan_simple_select(&self) -> SqlResult<PhysicalPlan> {
        Ok(PhysicalPlan {
            root: Box::new(SeqScan::new(
                discodb_types::TableId::min_value(),
                None,
                vec![],
            )),
        })
    }
}

impl Default for Planner {
    fn default() -> Self {
        Self::new()
    }
}
